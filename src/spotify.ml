(*
 * spotify.ml
 * ----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

open Lwt
open Lwt_react

let section = Lwt_log.Section.make "spotify"

(* +-----------------------------------------------------------------+
   | Misc                                                            |
   +-----------------------------------------------------------------+ *)

type ('a, 'b) either =
  | Inl of 'a
  | Inr of 'b

let rec split_either l =
  match l with
    | [] ->
        ([], [])
    | Inl x :: l ->
        let (a, b) = split_either l in
        (x :: a, b)
    | Inr x :: l ->
        let (a, b) = split_either l in
        (a, x :: b)

let rec make_filename l =
  match l with
    | [] ->
        ""
    | [x] ->
        x
    | x :: l ->
        Filename.concat x (make_filename l)

let xdg_cache_home =
  try
    Sys.getenv "XDG_CACHE_HOME"
  with Not_found ->
    let home = try Sys.getenv "HOME" with Not_found -> (Unix.getpwuid (Unix.getuid ())).Unix.pw_dir in
    if Sys.os_type = "Win32" then
      make_filename [home; "Local Settings"; "Cache"]
    else
      Filename.concat home ".cache"

let split sep str =
  let rec loop i j =
    if j = String.length str then
      [String.sub str i (j - i)]
    else
      if str.[j] = sep then
        String.sub str i (j - i) :: loop (j + 1) (j + 1)
      else
        loop i (j + 1)
  in
  loop 0 0

let strip str =
  let rec loop a =
    if a = String.length str then
      a
    else
      match str.[a] with
        | ' ' | '\n' | '\t' ->
            loop (a + 1)
        | _ ->
            a
  in
  let a = loop 0 in
  let rec loop b =
    if b = a then
      b
    else
      match str.[b - 1] with
        | ' ' | '\n' | '\t' ->
            loop (b - 1)
        | _ ->
            b
  in
  let b = loop (String.length str) in
  if a = 0 && b = String.length str then
    str
  else
    String.sub str a (b - a)

let split_space_coma str =
  let rec loop i j =
    if j = String.length str then
      [String.sub str i (j - i)]
    else
      match str.[j] with
        | ' ' | ',' ->
            String.sub str i (j - i) :: loop (j + 1) (j + 1)
        | _ ->
            loop i (j + 1)
  in
  loop 0 0

(* Ensure that a thread terminate before the end of the program. *)
let delay thread =
  if state thread = Sleep then begin
    let node = Lwt_sequence.add_r (fun () -> thread) Lwt_main.exit_hooks in
    ignore (
      try_lwt
        thread
      finally
        Lwt_sequence.remove node;
        return ()
    )
  end

let rec mkdir_p dir =
  try_lwt
    lwt () = Lwt_unix.access dir [Unix.F_OK] in
    return true
  with Unix.Unix_error _ ->
    lwt ok = mkdir_p (Filename.dirname dir) in
    if ok then
      try_lwt
        lwt () = Lwt_unix.mkdir dir 0o755 in
        return true
      with
        | Unix.Unix_error (Unix.EEXIST, _, _) ->
            return true
        | Unix.Unix_error (error, _, _) ->
            ignore (Lwt_log.error_f ~section "failed to make directory %S: %s" dir (Unix.error_message error));
            return false
    else
      return false

let safe_unlink file =
  try_lwt
    Lwt_unix.unlink file
  with Unix.Unix_error (error, _, _) ->
    ignore (Lwt_log.error_f ~section "cannot remove %S: %s" file (Unix.error_message error));
    return ()

let safe_close file fd =
  try_lwt
    Lwt_unix.close fd
  with Unix.Unix_error (error, _, _) ->
    ignore (Lwt_log.error_f ~section "cannot close %S: %s" file (Unix.error_message error));
    return ()

lwt dir_save_raw, debug =
  match try Some (Sys.getenv "MLSPOT_DEBUG_DIR") with Not_found -> None with
    | None ->
        return (None, false)
    | Some dir ->
        lwt ok = mkdir_p dir in
        lwt oc_types = Lwt_io.open_file ~mode:Lwt_io.output (Filename.concat dir "types")
        and oc_log = Lwt_io.open_file ~mode:Lwt_io.output (Filename.concat dir "log") in
        Lwt_log.add_rule "*" Lwt_log.Debug;
        Lwt_log.default := Lwt_log.broadcast [
          !Lwt_log.default;
          Lwt_log.channel ~template:"$(date).$(milliseconds): $(loc-file), line $(loc-line): $(message)" ~close_mode:`Close ~channel:oc_log ();
        ];
        return (Some (dir, oc_types), true)

let next_save_id = ref 0

let save_raw_data kind str =
  match dir_save_raw with
    | Some (dir, oc) ->
        let id = !next_save_id in
        next_save_id := id + 1;
        lwt () = Lwt_io.fprintlf oc "%08x: %s" id kind in
        Lwt_io.with_file ~mode:Lwt_io.output (Filename.concat dir (Printf.sprintf "%08x" id)) (fun oc -> Lwt_io.write oc str)
    | None ->
        return ()

(* +-----------------------------------------------------------------+
   | Cache versions                                                  |
   +-----------------------------------------------------------------+ *)

exception Cache_version_mismatch

let artist_cache_version = 0
let album_cache_version = 0
let track_cache_version = 0

(* +-----------------------------------------------------------------+
   | Errors                                                          |
   +-----------------------------------------------------------------+ *)

exception Offline
exception Logged_out
exception Disconnected
exception Connection_failure of string
exception Authentication_failure of string
exception Error of string

let () = Callback.register_exception "mlspot:error" (Error "")

(* +-----------------------------------------------------------------+
   | Shannon stream cipher                                           |
   +-----------------------------------------------------------------+ *)

type shn_ctx

external shn_ctx_new : unit -> shn_ctx = "mlspot_shn_ctx_new"
external shn_key : shn_ctx -> string -> int -> int -> unit = "mlspot_shn_key" "noalloc"
external shn_nonce : shn_ctx -> string -> int -> int -> unit = "mlspot_shn_nonce" "noalloc"
external shn_stream : shn_ctx -> string -> int -> int -> unit = "mlspot_shn_stream" "noalloc"
external shn_maconly : shn_ctx -> string -> int -> int -> unit = "mlspot_shn_maconly" "noalloc"
external shn_encrypt : shn_ctx -> string -> int -> int -> unit = "mlspot_shn_encrypt" "noalloc"
external shn_decrypt : shn_ctx -> string -> int -> int -> unit = "mlspot_shn_decrypt" "noalloc"
external shn_finish : shn_ctx -> string -> int -> int -> unit = "mlspot_shn_finish" "noalloc"

(* +-----------------------------------------------------------------+
   | AES                                                             |
   +-----------------------------------------------------------------+ *)

type aes_state

external rijndael_key_setup_enc : string -> aes_state = "mlspot_aes_rijndael_key_setup_enc"
external rijndael_key_setup_dec : string -> aes_state = "mlspot_aes_rijndael_key_setup_dec"
external rijndael_encrypt : aes_state -> string -> string -> unit = "mlspot_aes_rijndael_encrypt"
external rijndael_decrypt : aes_state -> string -> string -> unit = "mlspot_aes_rijndael_decrypt"

(* +-----------------------------------------------------------------+
   | Service lookup                                                  |
   +-----------------------------------------------------------------+ *)

external service_lookup_job : unit -> [ `service_lookup ] Lwt_unix.job = "mlspot_service_lookup_job"
external service_lookup_result : [ `service_lookup ] Lwt_unix.job -> (int * string * int) list = "mlspot_service_lookup_result"
external service_lookup_free : [ `service_lookup ] Lwt_unix.job -> unit = "mlspot_service_lookup_free" "noalloc"

let service_lookup () =
  Lwt_unix.execute_job (service_lookup_job ()) service_lookup_result service_lookup_free

(* +-----------------------------------------------------------------+
   | Reading/writing of unsigned integers in big endian              |
   +-----------------------------------------------------------------+ *)

exception Out_of_bounds

let put_int8 str ofs x =
  if ofs + 1 > String.length str then raise Out_of_bounds;
  String.unsafe_set str ofs (Char.unsafe_chr x)

let put_int16 str ofs x =
  if ofs + 2 > String.length str then raise Out_of_bounds;
  String.unsafe_set str (ofs + 0) (Char.unsafe_chr (x lsr 8));
  String.unsafe_set str (ofs + 1) (Char.unsafe_chr x)

let put_int32 str ofs x =
  if ofs + 4 > String.length str then raise Out_of_bounds;
  String.unsafe_set str (ofs + 0) (Char.unsafe_chr (x lsr 24));
  String.unsafe_set str (ofs + 1) (Char.unsafe_chr (x lsr 16));
  String.unsafe_set str (ofs + 2) (Char.unsafe_chr (x lsr 8));
  String.unsafe_set str (ofs + 3) (Char.unsafe_chr x)

let get_int8 str ofs =
  if ofs + 1 > String.length str then raise Out_of_bounds;
  Char.code (String.unsafe_get str ofs)

let get_int16 str ofs =
  if ofs + 2 > String.length str then raise Out_of_bounds;
  let x0 = Char.code (String.unsafe_get str (ofs + 0)) in
  let x1 = Char.code (String.unsafe_get str (ofs + 1)) in
  (x0 lsl 8) lor x1

let get_int32 str ofs =
  if ofs + 4 > String.length str then raise Out_of_bounds;
  let x0 = Char.code (String.unsafe_get str (ofs + 0)) in
  let x1 = Char.code (String.unsafe_get str (ofs + 1)) in
  let x2 = Char.code (String.unsafe_get str (ofs + 2)) in
  let x3 = Char.code (String.unsafe_get str (ofs + 3)) in
  (x0 lsl 24) lor (x1 lsl 16) lor (x2 lsl 8) lor x3

(* +-----------------------------------------------------------------+
   | Buffers                                                         |
   +-----------------------------------------------------------------+ *)

(* The Packet module is a simplified version of the Buffer module with
   primitives to append integers in big endian. *)
module Packet : sig
  type t
    (* Type of packets. *)

  val create : unit -> t
    (* Create a new empty packet. *)

  val length : t -> int
    (* Return the length of the given packet. *)

  val buffer : t -> string
    (* Return the internal buffer currently is use for the packet. *)

  val contents : t -> string
    (* Return the current contents of the packet. *)

  val reset : t -> unit
    (* Reset the packet length to 0. *)

  val add_string : t -> string -> unit
    (* Append the given string to the packet. *)

  val add_substring : t -> string -> int -> int -> unit
    (* Append the given sub-string to the packet. *)

  val add_int8 : t -> int -> unit
    (* Append an unsigned 8-bits integer to the packet. *)

  val add_int16 : t -> int -> unit
    (* Append an unsigned 16-bits integer to the packet, in big
       endian. *)

  val add_int32 : t -> int -> unit
    (* Append an unsigned 32-bits integer to the packet, in big
       endian. *)

  val put_int8 : t -> int -> int -> unit
    (* Write an unsigned 8-bits integer at the given offset in the
       packet. The offset + 1 must be smaller than the packet
       length. *)

  val put_int16 : t -> int -> int -> unit
    (* Write an unsigned 16-bits integer at the given offset in the
       packet. The offset + 2 must be smaller than the packet
       length. *)

  val put_int32 : t -> int -> int -> unit
    (* Write an unsigned 32-bits integer at the given offset in the
       packet. The offset + 4 must be smaller than the packet
       length. *)

  val send : Lwt_io.output_channel -> t -> unit Lwt.t
    (* Write the contents of the packet on the given output
       channel. *)

end = struct

  type t = {
    mutable buf : string;
    mutable ofs : int;
  }

  let create () = {
    buf = String.create 256;
    ofs = 0;
  }

  let length packet = packet.ofs
  let buffer packet = packet.buf

  let contents packet = String.sub packet.buf 0 packet.ofs

  let reset packet = packet.ofs <- 0

  let extend packet size =
    if packet.ofs + size > String.length packet.buf then begin
      let buf = String.create (max (String.length packet.buf * 2) (String.length packet.buf + size)) in
      String.unsafe_blit packet.buf 0 buf 0 packet.ofs;
      packet.buf <- buf
    end

  let add_string packet str =
    let len = String.length str in
    extend packet len;
    String.unsafe_blit str 0 packet.buf packet.ofs len;
    packet.ofs <- packet.ofs + len

  let add_substring packet str ofs len =
    extend packet len;
    String.blit str ofs packet.buf packet.ofs len;
    packet.ofs <- packet.ofs + len

  let add_int8 packet x =
    extend packet 1;
    String.unsafe_set packet.buf packet.ofs (Char.unsafe_chr x);
    packet.ofs <- packet.ofs + 1

  let add_int16 packet x =
    extend packet 2;
    String.unsafe_set packet.buf (packet.ofs + 0) (Char.unsafe_chr (x lsr 8));
    String.unsafe_set packet.buf (packet.ofs + 1) (Char.unsafe_chr x);
    packet.ofs <- packet.ofs + 2

  let add_int32 packet x =
    extend packet 4;
    String.unsafe_set packet.buf (packet.ofs + 0) (Char.unsafe_chr (x asr 24));
    String.unsafe_set packet.buf (packet.ofs + 1) (Char.unsafe_chr (x lsr 16));
    String.unsafe_set packet.buf (packet.ofs + 2) (Char.unsafe_chr (x lsr 8));
    String.unsafe_set packet.buf (packet.ofs + 3) (Char.unsafe_chr x);
    packet.ofs <- packet.ofs + 4

  let put_int8 packet ofs x =
    if ofs + 1 > packet.ofs then invalid_arg "Spotify.Packet.put_int8";
    String.unsafe_set packet.buf ofs (Char.unsafe_chr x)

  let put_int16 packet ofs x =
    if ofs + 2 > packet.ofs then invalid_arg "Spotify.Packet.put_int16";
    String.unsafe_set packet.buf (ofs + 0) (Char.unsafe_chr (x lsr 8));
    String.unsafe_set packet.buf (ofs + 1) (Char.unsafe_chr x)

  let put_int32 packet ofs x =
    if ofs + 4 > packet.ofs then invalid_arg "Spotify.Packet.put_int32";
    String.unsafe_set packet.buf (ofs + 0) (Char.unsafe_chr (x lsr 24));
    String.unsafe_set packet.buf (ofs + 1) (Char.unsafe_chr (x lsr 16));
    String.unsafe_set packet.buf (ofs + 2) (Char.unsafe_chr (x lsr 8));
    String.unsafe_set packet.buf (ofs + 3) (Char.unsafe_chr x)

  let send oc packet =
    Lwt_io.write_from_exactly oc packet.buf 0 packet.ofs
end

(* +-----------------------------------------------------------------+
   | Byte streams                                                    |
   +-----------------------------------------------------------------+ *)

(* Type of byte stream. *)
class type byte_stream = object
  method read : string -> int -> int -> int Lwt.t
    (* [read buf ofs len] reads up to [len] bytes and store them into
       [buf] at [ofs]. It returns the number of bytes actually
       read. It returns [0] on end of stream.

       Notes:
       - all calls to read must be serialized
       - it must raise only [Error] *)

  method close : unit Lwt.t
    (* Close the stream.

       Note:
       - there must be only one call to close, and it must be done after all
       calls to [read]
       - it must raise only [Error] *)
end

(* Read all data of the given stream. *)
let string_of_stream stream =
  let rec loop size data =
    let buffer = String.create 4096 in
    match_lwt stream#read buffer 0 4096 with
      | 0 ->
          lwt () = stream#close in
          let res = String.create size in
          let rec copy ofs data =
            match data with
              | [] ->
                  return res
              | (buf, len) :: data ->
                  let ofs = ofs - len in
                  String.blit buf 0 res ofs len;
                  copy ofs data
          in
          copy size data
      | n ->
          loop (size + n) ((buffer, n) :: data)
  in
  loop 0 []

(* Create a stream from a sub-string. *)
class stream_of_string string init_offset init_length = object
  val mutable offset = init_offset
  val mutable length = init_length

  method read buf ofs len =
    let len = min len length in
    String.blit string offset buf ofs len;
    offset <- offset + len;
    length <- length - len;
    return len

  method close =
    return ()
end

(* Create a stream from a file descriptor. [file] is used for error
   messages. *)
class stream_of_file file fd = object
  method read buf ofs len =
    try_lwt
      Lwt_unix.read fd buf ofs len
    with Unix.Unix_error (error, _, _) ->
      raise_lwt (Error (Printf.sprintf "cannot read from %S: %s" file (Unix.error_message error)))

  method close =
    try_lwt
      Lwt_unix.close fd
    with Unix.Unix_error (error, _, _) ->
      raise_lwt (Error (Printf.sprintf "cannot close %S: %s" file (Unix.error_message error)))
end

type stream_state = BOS | BODY | EOS
    (* State of stream:

       - BOS = beginning of stream
       - BODy = body of the stream
       - EOS = end of stream *)

class inflate_stream (stream : byte_stream) = object(self)
  val buffer = String.create 4096
    (* Buffer containing compressed data. *)

  val mutable offset = 0
    (* Offset of data not yet consumed in [buffer]. *)

  val mutable length = 0
    (* Length of data in [buffer]. *)

  val z_stream = Zlib.inflate_init false
    (* The Z stream. *)

  val mutable size = 0l
    (* Size of uncomressed data read so far. *)

  val mutable crc = 0l
    (* CRC of uncompressed data read so far. *)

  val mutable state = BOS
    (* State of the stream. *)

  method private read_byte =
    if length > 0 then begin
      let byte = Char.code buffer.[offset] in
      offset <- offset + 1;
      length <- length - 1;
      return byte
    end else begin
      match_lwt stream#read buffer 0 (String.length buffer) with
        | 0 ->
            raise_lwt (Error "invalid gzip file: premature end of data")
        | n ->
            offset <- 1;
            length <- n - 1;
            return (Char.code buffer.[0])
    end

  method private read_int32_little_endian =
    lwt x0 = self#read_byte >|= Int32.of_int in
    lwt x1 = self#read_byte >|= Int32.of_int in
    lwt x2 = self#read_byte >|= Int32.of_int in
    lwt x3 = self#read_byte >|= Int32.of_int in
    return (Int32.logor x0 (Int32.logor (Int32.shift_left x1 8) (Int32.logor (Int32.shift_left x2 16) (Int32.shift_left x3 24))))

  method private skip_string =
    match_lwt self#read_byte with
      | 0 -> return ()
      | _ -> self#skip_string

  method private read_header =
    lwt id1 = self#read_byte in
    lwt id2 = self#read_byte in
    if id1 <> 0x1f || id2 <> 0x8b then raise (Error "invalid gzip file: bad magic number");
    lwt cm = self#read_byte in
    if cm <> 8 then raise (Error "invalid gzip file: unknown compression method");
    lwt flags = self#read_byte in
    if flags land 0xe0 <> 0 then raise (Error "invalid gzip file: bad flags");
    lwt () = for_lwt i = 1 to 6 do self#read_byte >> return () done in
    lwt () =
      if flags land 0x04 <> 0 then begin
        lwt len1 = self#read_byte in
        lwt len2 = self#read_byte in
        for_lwt i = 1 to len1 lor (len2 lsl 8) do
          lwt _ = self#read_byte in
          return ()
        done
      end else
        return ()
    in
    lwt () =
      if flags land 0x08 <> 0 then
        self#skip_string
      else
        return ()
    in
    lwt () =
      if flags land 0x10 <> 0 then
        self#skip_string
      else
        return ()
    in
    if flags land 0x02 <> 0 then
      lwt _ = self#read_byte in
      lwt _ = self#read_byte in
      return ()
    else
      return ()

  method private check_data =
    lwt crc' = self#read_int32_little_endian in
    lwt size' = self#read_int32_little_endian in
    if crc <> crc' then
      raise_lwt (Error "invalid gzip file: CRC mismatch")
    else if size <> size' then
      raise_lwt (Error "invalid gzip file: size mismatch")
    else
      return ()

  method read buf ofs len =
    match state with
      | BOS ->
          lwt () = self#read_header in
          state <- BODY;
          self#read buf ofs len
      | EOS ->
          return 0
      | BODY ->
          if length > 0 then begin
            let eos, len_src, len_dst =
              try
                Zlib.inflate z_stream buffer offset length buf ofs len Zlib.Z_SYNC_FLUSH
              with Zlib.Error (func, message) ->
                raise (Error (Printf.sprintf "invalid gzip file: %s" message))
            in
            offset <- offset + len_src;
            length <- length - len_src;
            size <- Int32.add size (Int32.of_int len_dst);
            crc <- Zlib.update_crc crc buf ofs len_dst;
            if eos then begin
              Zlib.inflate_end z_stream;
              state <- EOS;
              lwt () = self#check_data in
              return len_dst
            end else if len_dst = 0 then
              self#read buf ofs len
            else
              return len_dst
          end else begin
            match_lwt stream#read buffer 0 (String.length buffer) with
              | 0 ->
                  raise_lwt (Error "invalid gzip file: premature end of data")
              | n ->
                  offset <- 0;
                  length <- n;
                  self#read buf ofs len
          end

  method close =
    stream#close
end

(* +-----------------------------------------------------------------+
   | XML                                                             |
   +-----------------------------------------------------------------+ *)

class xml_parser = object
  method node (tag : string) (attrs : (string * string) list) =
    ignore (Lwt_log.debug_f ~section "no element expected here, got %S" tag);
    new xml_parser

  method data (str : string) =
    ignore (Lwt_log.debug ~section "no data expected here")

  method stop =
    ()
end

class data assign = object
  inherit xml_parser

  val mutable data = None

  method data str =
    match data with
      | Some _ ->
          ignore (Lwt_log.warning_f ~section "too many data, discarding previous one");
          data <- Some str
      | None ->
          data <- Some str

  method stop =
    match data with
      | None ->
          ()
      | Some str ->
          try
            assign str
          with
            | Cache_version_mismatch as exn ->
                raise exn
            | exn ->
                ignore (Lwt_log.error_f ~section ~exn "failed to parse XML data")
end

class node name f = object
  inherit xml_parser

  method node tag attrs =
    if name = tag then
      f attrs
    else
      new xml_parser

  method stop =
    ()
end

class ['a] nodes name f assign = object
  inherit xml_parser

  val mutable elements : 'a list = []

  method node tag attrs =
    if tag = name then
      f attrs (fun x -> elements <- x :: elements)
    else
      new xml_parser

  method stop =
    assign (List.rev elements)
end

module XML = struct

  (* +---------------------------------------------------------------+
     | XML parsing                                                   |
     +---------------------------------------------------------------+ *)

  (* A data maker. It collects chunks of data and put them
     together. *)
  type data_maker = {
    mutable data_length : int;
    (* Length of all data in the maker. *)
    mutable data_chunks : string list;
    (* Chunks of data read. *)
  }

  (* Concatanates all chunks of a data maker. *)
  let make_data dm =
    match dm.data_chunks with
      | [] ->
          ""
      | [str] ->
          dm.data_length <- 0;
          dm.data_chunks <- [];
          str
      | _ ->
          let res = String.create dm.data_length in
          let rec loop ofs l =
            match l with
              | [] ->
                  (* Empty the maker for reuse. *)
                  dm.data_length <- 0;
                  dm.data_chunks <- [];
                  res
              | str :: l ->
                  let len = String.length str in
                  let ofs = ofs - len in
                  String.unsafe_blit str 0 res ofs len;
                  loop ofs l
          in
          loop dm.data_length dm.data_chunks

  let create_parser root_handler =
    let xp = Expat.parser_create None in
    let stack = ref [] in
    let handler = ref root_handler in
    let dm = { data_length = 0; data_chunks = [] } in
    Expat.set_start_element_handler xp
      (fun name attrs ->
         (* Handle remaining data. *)
         if dm.data_length > 0 then !handler#data (make_data dm);
         (* Push the current handler to the stack. *)
         stack := !handler :: !stack;
         (* Create a new handler for the sub-node. *)
         handler := !handler#node name attrs);
    Expat.set_end_element_handler xp
      (fun name ->
         (* Handle remaining data. *)
         if dm.data_length > 0 then !handler#data (make_data dm);
         (* Stop parsing this node. *)
         !handler#stop;
         (* Restore the previous handler. *)
         match !stack with
           | [] ->
               assert false
           | x :: l ->
               handler := x;
               stack := l);
    Expat.set_character_data_handler xp
      (fun str ->
         (* Put data in the maker. *)
         dm.data_length <- dm.data_length + String.length str;
         dm.data_chunks <- str :: dm.data_chunks);
    xp

  let buffer_size = 8192

  let parse_stream ?(buggy_root = false) cell root_handler stream =
    let root_handler = if buggy_root then new node "root" (fun attrs -> root_handler) else root_handler in
    let xp = create_parser root_handler in
    let buffer = String.create 4096 in
    let rec loop () =
      match_lwt stream#read buffer 0 (String.length buffer) with
        | 0 ->
            if buggy_root then Expat.parse xp "</root>";
            Expat.final xp;
            root_handler#stop;
            return (match !cell with
                      | Some x -> x
                      | None -> assert false)
        | n ->
            Expat.parse_sub xp buffer 0 n;
            loop ()
    in
    try_lwt
      if buggy_root then Expat.parse xp "<root>";
      loop ()
    with Expat.Expat_error error ->
      raise_lwt (Error ("invalid XML file: " ^ Expat.xml_error_to_string error))
    finally
      stream#close

  let parse_string ?(buggy_root = false) cell root_handler string =
    let root_handler = if buggy_root then new node "root" (fun attrs -> root_handler) else root_handler in
    let xp = create_parser root_handler in
    try
      if buggy_root then Expat.parse xp "<root>";
      Expat.parse xp string;
      if buggy_root then Expat.parse xp "</root>";
      Expat.final xp;
      root_handler#stop;
      match !cell with
        | Some x -> x
        | None -> assert false
    with Expat.Expat_error error ->
      raise (Error ("invalid XML file: " ^ Expat.xml_error_to_string error))

  (* +---------------------------------------------------------------+
     | XML writer                                                    |
     +---------------------------------------------------------------+ *)

  (* Type of a XML writer. It writes to a gzipped file. *)
  type writer = {
    fd : Lwt_unix.file_descr;
    (* The file descriptor used for the output. *)
    file : string;
    (* The file we are writing to. In case of error it is erased. *)
    stream : Zlib.stream;
    (* The Zlib stream. *)
    mutable indent : int;
    (* Current indentiation. *)
    mutable iofs : int;
    (* Offset of end of data in the uncompressed buffer. *)
    mutable oofs : int;
    (* Offset of end of data in the compressed buffer. *)
    ibuffer : string;
    (* The uncompressed buffer. *)
    obuffer : string;
    (* The compressed buffer. *)
    mutable size : int32;
    (* Size of data written so far. *)
    mutable crc : int32;
    (* CRC of data written so far. *)
  }

  let create_writer fd file =
    let oc = {
      fd = fd;
      file = file;
      stream = Zlib.deflate_init 9 false;
      indent = 0;
      iofs = 0;
      oofs = 10;
      ibuffer = String.create buffer_size;
      obuffer = String.create (buffer_size + 8);
      size = Int32.zero;
      crc = Int32.zero;
    } in
    (* Write header. *)
    put_int8 oc.obuffer 0 0x1f;
    put_int8 oc.obuffer 1 0x8b;
    put_int8 oc.obuffer 2 8;
    put_int8 oc.obuffer 3 0;
    put_int32 oc.obuffer 4 0;
    put_int8 oc.obuffer 8 0;
    put_int8 oc.obuffer 9 0xff;
    oc

  let rec write oc str ofs len =
    let avail = buffer_size - oc.iofs in
    if len <= avail then begin
      String.blit str ofs oc.ibuffer oc.iofs len;
      oc.iofs <- oc.iofs + len;
      return ()
    end else begin
      String.blit str ofs oc.ibuffer oc.iofs avail;
      (* Compress the input buffer. *)
      let _, len_src, len_dst = Zlib.deflate oc.stream oc.ibuffer 0 buffer_size oc.obuffer oc.oofs (buffer_size - oc.oofs) Zlib.Z_NO_FLUSH in
      (* Update size and CRC. *)
      oc.size <- Int32.add oc.size (Int32.of_int len_src);
      oc.crc <- Zlib.update_crc oc.crc oc.ibuffer 0 len_src;
      (* Write as much as possible. *)
      lwt n = Lwt_unix.write oc.fd oc.obuffer 0 (oc.oofs + len_dst) in
      (* Update buffers and offsets. *)
      let iremaining = buffer_size - len_src in
      let oremaining = oc.oofs + len_dst - n in
      if iremaining <> 0 then String.blit oc.ibuffer len_src oc.ibuffer 0 iremaining;
      if oremaining <> 0 then String.blit oc.obuffer n oc.obuffer 0 oremaining;
      oc.iofs <- iremaining;
      oc.oofs <- oremaining;
      (* Write remaining data. *)
      write oc str (ofs + avail) (len - avail)
    end

  let write_string oc str = write oc str 0 (String.length str)

  let rec write_exactly fd str ofs len =
    if len = 0 then
      return ()
    else
      lwt n = Lwt_unix.write fd str ofs len in
      write_exactly fd str (ofs + n) (len - n)

  let rec finish_deflate oc iofs =
    let finished, len_src, len_dst = Zlib.deflate oc.stream oc.ibuffer iofs (oc.iofs - iofs) oc.obuffer oc.oofs (buffer_size - oc.oofs) Zlib.Z_FINISH in
    (* Update size and CRC. *)
    oc.size <- Int32.add oc.size (Int32.of_int len_src);
    oc.crc <- Zlib.update_crc oc.crc oc.ibuffer iofs len_src;
    if not finished then begin
      (* Flush the output buffer. *)
      lwt n = Lwt_unix.write oc.fd oc.obuffer 0 (oc.oofs + len_dst) in
      (* Update the buffer and the offset. *)
      let oremaining = oc.oofs + len_dst - n in
      if oremaining <> 0 then String.blit oc.obuffer n oc.obuffer 0 oremaining;
      oc.oofs <- oremaining;
      finish_deflate oc (iofs + len_src)
    end else begin
      oc.oofs <- oc.oofs + len_dst;
      return ()
    end

  let put_int32_little_endian str ofs x =
    String.unsafe_set str (ofs + 0) (Char.unsafe_chr (Int32.to_int x));
    String.unsafe_set str (ofs + 1) (Char.unsafe_chr (Int32.to_int (Int32.shift_right_logical x 8)));
    String.unsafe_set str (ofs + 2) (Char.unsafe_chr (Int32.to_int (Int32.shift_right_logical x 16)));
    String.unsafe_set str (ofs + 3) (Char.unsafe_chr (Int32.to_int (Int32.shift_right_logical x 24)))

  let close oc =
    lwt () = finish_deflate oc 0 in
    (* Dispose of the stream. *)
    Zlib.deflate_end oc.stream;
    (* Write CRC and size. *)
    put_int32_little_endian oc.obuffer oc.oofs oc.crc;
    put_int32_little_endian oc.obuffer (oc.oofs + 4) oc.size;
    (* Flush the buffer. *)
    lwt () = write_exactly oc.fd oc.obuffer 0 (oc.oofs + 8) in
    (* Close the file. *)
    safe_close oc.file oc.fd

  let abort oc =
    lwt () = safe_close oc.file oc.fd in
    safe_unlink oc.file

  let rec write_indent oc n =
    if n < oc.indent then
      lwt () = write_string oc "  " in
      write_indent oc (n + 1)
    else
      return ()

  let rec write_cdata oc data i j =
    if j = String.length data then
      if i < j then
        write oc data i (j - i)
      else
        return ()
    else
      match data.[j] with
        | '"' ->
            if i < j then
              lwt () = write oc data i (j - i) in
              lwt () = write_string oc "&quot;" in
              write_cdata oc data (j + 1) (j + 1)
            else
              lwt () = write_string oc "&quot;" in
              write_cdata oc data (j + 1) (j + 1)
        | '<' ->
            if i < j then
              lwt () = write oc data i (j - i) in
              lwt () = write_string oc "&lt;" in
              write_cdata oc data (j + 1) (j + 1)
            else
              lwt () = write_string oc "&lt;" in
              write_cdata oc data (j + 1) (j + 1)
        | '>' ->
            if i < j then
              lwt () = write oc data i (j - i) in
              lwt () = write_string oc "&gt;" in
              write_cdata oc data (j + 1) (j + 1)
            else
              lwt () = write_string oc "&gt;" in
              write_cdata oc data (j + 1) (j + 1)
        | '&' ->
            if i < j then
              lwt () = write oc data i (j - i) in
              lwt () = write_string oc "&amp;" in
              write_cdata oc data (j + 1) (j + 1)
            else
              lwt () = write_string oc "&amp;" in
              write_cdata oc data (j + 1) (j + 1)
        | _ ->
            write_cdata oc data i (j + 1)

  let write_data oc tag data =
    lwt () = write_indent oc 0 in
    lwt () = write_string oc "<" in
    lwt () = write_string oc tag in
    lwt () = write_string oc ">" in
    lwt () = write_cdata oc data 0 0 in
    lwt () = write_string oc "</" in
    lwt () = write_string oc tag in
    lwt () = write_string oc ">\n" in
    return ()

  let write_node oc tag attrs f =
    lwt () = write_indent oc 0 in
    lwt () = write_string oc "<" in
    lwt () = write_string oc tag in
    lwt () = Lwt_list.iter_s (fun (name, value) ->
                                lwt () = write_string oc " " in
                                lwt () = write_string oc name in
                                lwt () = write_string oc "=\"" in
                                lwt () = write_cdata oc value 0 0 in
                                lwt () = write_string oc "\"" in
                                return ()) attrs in
    lwt () = write_string oc ">\n" in
    oc.indent <- oc.indent + 1;
    lwt () = f oc in
    oc.indent <- oc.indent - 1;
    lwt () = write_indent oc 0 in
    lwt () = write_string oc "</" in
    lwt () = write_string oc tag in
    lwt () = write_string oc ">\n" in
    return ()

  let write_empty_node oc tag attrs =
    lwt () = write_indent oc 0 in
    lwt () = write_string oc "<" in
    lwt () = write_string oc tag in
    lwt () = Lwt_list.iter_s (fun (name, value) ->
                                lwt () = write_string oc " " in
                                lwt () = write_string oc name in
                                lwt () = write_string oc "=\"" in
                                lwt () = write_cdata oc value 0 0 in
                                lwt () = write_string oc "\"" in
                                return ()) attrs in
    lwt () = write_string oc "/>\n" in
    return ()

  let write_nodes oc tag writer l =
    write_node oc tag [] (fun oc -> Lwt_list.iter_s (fun x -> writer oc x) l)
end

(* +-----------------------------------------------------------------+
   | Product informations                                            |
   +-----------------------------------------------------------------+ *)

class product product_type expiry = object
  method product_type : string = product_type
  method expiry : int option = expiry
end

class product_parser assign = object
  inherit xml_parser

  val mutable product_type = ""
  val mutable expiry = None

  method node tag attrs =
    match tag with
      | "type" -> new data (fun str -> product_type <- str)
      | "expiry" -> new data (fun str -> expiry <- if str = "None" then None else Some (int_of_string str))
      | _ -> new xml_parser

  method stop =
    assign (new product product_type expiry)
end

class products_parser assign =
  node
    "products"
    (fun attrs -> new nodes "product" (fun attrs assign -> new product_parser assign) assign)

(* +-----------------------------------------------------------------+
   | IDs                                                             |
   +-----------------------------------------------------------------+ *)

exception Id_parse_failure
exception Wrong_id of string

module ID : sig
  type t
    (* Note: ID are comparable using (==). *)

  val length : t -> int
  val hash : t -> int

  val of_string : string -> t
  val to_string : t -> string

  val to_bytes : t -> string
    (* Warning: the result must not be modified. *)

  val of_bytes : string -> t
    (* Warning: the string must not be modified. *)
end = struct

  type t = {
    bytes : string;
    hash : int;
  }

  type id = t

  module IDs = Weak.Make (struct
                            type t = id
                            let equal id1 id2 = id1.bytes = id2.bytes
                            let hash id = id.hash
                          end)

  (* Weak table of all encountered ids. *)
  let ids = IDs.create 16384

  let length id = String.length id.bytes
  let hash id = id.hash

  let to_bytes id = id.bytes
  let of_bytes bytes = IDs.merge ids { bytes; hash = Hashtbl.hash bytes }

  let int_of_hexa = function
    | '0' .. '9' as ch -> Char.code ch - Char.code '0'
    | 'a' .. 'f' as ch -> Char.code ch - Char.code 'a' + 10
    | 'A' .. 'F' as ch -> Char.code ch - Char.code 'A' + 10
    | _ -> raise Id_parse_failure

  let of_string str =
    let len = String.length str in
    if len land 1 <> 0 then raise Id_parse_failure;
    let bytes = String.create (len / 2) in
    for i = 0 to len / 2 - 1 do
      let x0 = int_of_hexa (String.unsafe_get str (i * 2 + 0)) in
      let x1 = int_of_hexa (String.unsafe_get str (i * 2 + 1)) in
      String.unsafe_set bytes i (Char.unsafe_chr ((x0 lsl 4) lor x1))
    done;
    of_bytes bytes

  let hexa_of_int n =
    if n < 10 then
      Char.unsafe_chr (Char.code '0' + n)
    else
      Char.unsafe_chr (Char.code 'a' + n - 10)

  let to_string id =
    let len = String.length id.bytes in
    let str = String.create (len * 2) in
    for i = 0 to len - 1 do
      let x = Char.code (String.unsafe_get id.bytes i) in
      String.unsafe_set str (i * 2 + 0) (hexa_of_int (x lsr 4));
      String.unsafe_set str (i * 2 + 1) (hexa_of_int (x land 15))
    done;
    str
end

type id = ID.t
let id_length = ID.length
let id_hash = ID.hash
let id_of_string = ID.of_string
let string_of_id = ID.to_string

module ID_table = Hashtbl.Make (struct
                                  type t = id
                                  let equal = ( == )
                                  let hash = id_hash
                                end)

(* +-----------------------------------------------------------------+
   | Links                                                           |
   +-----------------------------------------------------------------+ *)

type uri = string

let base62_alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

let base62_of_code x = base62_alphabet.[x]

let code_of_base62 = function
  | '0' -> 0
  | '1' -> 1
  | '2' -> 2
  | '3' -> 3
  | '4' -> 4
  | '5' -> 5
  | '6' -> 6
  | '7' -> 7
  | '8' -> 8
  | '9' -> 9
  | 'a' -> 10
  | 'b' -> 11
  | 'c' -> 12
  | 'd' -> 13
  | 'e' -> 14
  | 'f' -> 15
  | 'g' -> 16
  | 'h' -> 17
  | 'i' -> 18
  | 'j' -> 19
  | 'k' -> 20
  | 'l' -> 21
  | 'm' -> 22
  | 'n' -> 23
  | 'o' -> 24
  | 'p' -> 25
  | 'q' -> 26
  | 'r' -> 27
  | 's' -> 28
  | 't' -> 29
  | 'u' -> 30
  | 'v' -> 31
  | 'w' -> 32
  | 'x' -> 33
  | 'y' -> 34
  | 'z' -> 35
  | 'A' -> 36
  | 'B' -> 37
  | 'C' -> 38
  | 'D' -> 39
  | 'E' -> 40
  | 'F' -> 41
  | 'G' -> 42
  | 'H' -> 43
  | 'I' -> 44
  | 'J' -> 45
  | 'K' -> 46
  | 'L' -> 47
  | 'M' -> 48
  | 'N' -> 49
  | 'O' -> 50
  | 'P' -> 51
  | 'Q' -> 52
  | 'R' -> 53
  | 'S' -> 54
  | 'T' -> 55
  | 'U' -> 56
  | 'V' -> 57
  | 'W' -> 58
  | 'X' -> 59
  | 'Y' -> 60
  | 'Z' -> 61
  | _ -> raise Exit

let base_convert src of_base to_base dst_len =
  let dst = Array.create dst_len 0 in
  let rec loop idx len =
    let divide = ref 0 and newlen = ref 0 in
    for i = 0 to len - 1 do
      divide := !divide * of_base + src.(i);
      if !divide >= to_base then begin
        src.(!newlen) <- !divide / to_base;
        incr newlen;
        divide := !divide mod to_base
      end else if !newlen > 0 then begin
        src.(!newlen) <- 0;
        incr newlen
      end
    done;
    dst.(idx) <- !divide;
    if !newlen <> 0 then loop (idx - 1) !newlen
  in
  loop (dst_len - 1) (Array.length src);
  dst

let base62_encode bytes len =
  let src = Array.create (String.length bytes) 0 in
  for i = 0 to String.length bytes - 1 do
    src.(i) <- Char.code bytes.[i]
  done;
  let dst = base_convert src 256 62 len in
  let res = String.create len in
  for i = 0 to len - 1 do
    res.[i] <- base62_of_code dst.(i)
  done;
  res

let base62_decode str len =
  let src = Array.create (String.length str) 0 in
  for i = 0 to String.length str - 1 do
    src.(i) <- code_of_base62 str.[i]
  done;
  let dst = base_convert src 62 256 len in
  let res = String.create len in
  for i = 0 to len - 1 do
    res.[i] <- Char.chr dst.(i)
  done;
  res

type link =
  | Track of id
  | Album of id
  | Artist of id
  | Search of string
  | Playlist of string * id
  | Image of id

exception Invalid_uri of string

let link_of_uri uri =
  try
    match split ':' uri with
      | ["spotify"; "track"; arg] when String.length arg = 22 ->
          Track (ID.of_bytes (base62_decode arg 16))
      | ["spotify"; "album"; arg] when String.length arg = 22 ->
          Album (ID.of_bytes (base62_decode arg 16))
      | ["spotify"; "artist"; arg] when String.length arg = 22 ->
          Artist (ID.of_bytes (base62_decode arg 16))
      | ["search"; arg] ->
          Search arg
      | ["spotify"; "user"; user; "playlist"; arg] when String.length arg = 22 ->
          Playlist (user, ID.of_bytes (base62_decode arg 16))
      | ["spotify"; "image"; arg] when String.length arg = 40 ->
          Image (id_of_string arg)
      | _ ->
          raise Exit
  with _ ->
    raise (Invalid_uri uri)

let uri_of_link = function
  | Track id ->
      String.concat ":" ["spotify"; "track"; base62_encode (ID.to_bytes id) 22]
  | Album id ->
      String.concat ":" ["spotify"; "album"; base62_encode (ID.to_bytes id) 22]
  | Artist id ->
      String.concat ":" ["spotify"; "artist"; base62_encode (ID.to_bytes id) 22]
  | Search str ->
      String.concat ":" ["spotify"; "search"; str]
  | Playlist (user, id) ->
      String.concat ":" ["spotify"; "user"; user; "playlist"; base62_encode (ID.to_bytes id) 22]
  | Image id ->
      String.concat ":" ["spotify"; "image"; string_of_id id]

(* +-----------------------------------------------------------------+
   | Cache                                                           |
   +-----------------------------------------------------------------+ *)

module Weak_cache : sig
  type 'a t
    (* Type of object catching element of type ['a], indexed by an
       ID. Element must be finalisable. *)

  val create : int -> 'a t
    (* Create a new empty cache. *)

  val get : 'a t -> id -> 'a option
    (* [get cache id] returns the element associated to [id] in
       [cache] if any. *)

  val add : 'a t -> id -> 'a -> unit
    (* [add cache id x] binds [id] to [x] in [cache] if [id] is not
       already bound. *)

  val find : 'a t -> id -> (unit -> 'a) -> 'a
    (* [find cache id make] search for an element with id [id] in
       [cache]. If one is found it is returned, otherwise a new one is
       created with [make], added to the cache and returned.

       Note that the cache keeps only a weak reference to the element,
       so its is removed when collected. *)

  val find_lwt : 'a t -> id -> (unit -> 'a Lwt.t) -> 'a Lwt.t
    (* Same as find but [make] returns a thread. *)

  val find_multiple_lwt : (< id : id; .. > as 'a) t -> id list -> (id list -> 'a list Lwt.t) -> 'a list Lwt.t
    (* [find_multiple_lwt cache ids make] returns all elements found
       in the cache, creating missing ones with [make]. *)
end = struct
  type 'a t = 'a Weak.t ID_table.t

  let create size = ID_table.create size

  let finalise cache id obj =
    ID_table.remove cache id

  let get cache id =
    try
      Weak.get (ID_table.find cache id) 0
    with Not_found ->
      None

  let add cache id x =
    try
      let weak = ID_table.find cache id in
      if Weak.get weak 0 = None then
        Weak.set weak 0 (Some x)
    with Not_found ->
      let weak = Weak.create 1 in
      Weak.set weak 0 (Some x);
      ID_table.add cache id weak;
      Gc.finalise (finalise cache id) x

  let find cache id make =
    try
      let weak = ID_table.find cache id in
      match Weak.get weak 0 with
        | Some data ->
            data
        | None ->
            let data = make () in
            Weak.set weak 0 (Some data);
            data
    with Not_found ->
      let data = make () in
      let weak = Weak.create 1 in
      Weak.set weak 0 (Some data);
      ID_table.add cache id weak;
      Gc.finalise (finalise cache id) data;
      data

  let find_lwt cache id make =
    try
      let weak = ID_table.find cache id in
      match Weak.get weak 0 with
        | Some data ->
            return data
        | None ->
            lwt data = make () in
            Weak.set weak 0 (Some data);
            return data
    with Not_found ->
      lwt data = make () in
      let weak = Weak.create 1 in
      Weak.set weak 0 (Some data);
      ID_table.add cache id weak;
      Gc.finalise (finalise cache id) data;
      return data

  let find_multiple_lwt cache ids make =
    let old_datas, ids =
      split_either
        (List.map
           (fun id ->
              try
                match Weak.get (ID_table.find cache id) 0 with
                  | Some data ->
                      Inl data
                  | None ->
                      Inr id
              with Not_found ->
                Inr id)
           ids)
    in
    lwt new_datas = make ids in
    List.iter
      (fun data ->
         let weak = Weak.create 1 in
         Weak.set weak 0 (Some data);
         ID_table.add cache data#id weak;
         Gc.finalise (finalise cache data#id) data)
      new_datas;
    return (old_datas @ new_datas)
end

(* +-----------------------------------------------------------------+
   | Structures                                                      |
   +-----------------------------------------------------------------+ *)

class portrait id link width height = object
  method id : id = id
  method link : link = link
  method width : int = width
  method height : int = height
end

class biography text portraits = object
  method text : string = text
  method portraits : portrait list = portraits
end

class restriction catalogues forbidden allowed = object
  method catalogues : string list option = catalogues
  method forbidden : string list option = forbidden
  method allowed : string list option = allowed
end

class similar_artist id link name portrait genres years_active restrictions = object
  method id : id = id
  method link : link = link
  method name : string = name
  method portrait : id option = portrait
  method genres : string list = genres
  method years_active : int list = years_active
  method restrictions : restriction list = restrictions
end

class file id format bitrate = object
  method id : id = id
  method format : string = format
  method bitrate : int = bitrate
end

class alternative id link files restrictions = object
  method id : id = id
  method link : link = link
  method files : file list = files
  method restrictions : restriction list = restrictions
end

class track id link title explicit artists artists_id album album_id album_artist album_artist_id year number length files cover popularity external_ids alternatives  = object
  method id : id = id
  method link : link = link
  method title : string = title
  method explicit : bool = explicit
  method artists : string list = artists
  method artists_id : id list = artists_id
  method album : string = album
  method album_id : id = album_id
  method album_artist : string = album_artist
  method album_artist_id : id = album_artist_id
  method year : int = year
  method number : int = number
  method length : float = length
  method files : file list = files
  method cover : id option = cover
  method popularity : float = popularity
  method external_ids : (string * string) list = external_ids
  method alternatives : alternative list = alternatives
end

class disc number name tracks = object
  method number : int = number
  method name : string option = name
  method tracks : id list = tracks
end

class album id link name artist artist_id album_type year cover copyrights restrictions external_ids discs = object
  method id : id = id
  method link : link = link
  method name : string = name
  method artist : string = artist
  method artist_id : id = artist_id
  method album_type : string = album_type
  method year : int = year
  method cover : id option = cover
  method copyrights : (string * string) list = copyrights
  method restrictions : restriction list = restrictions
  method external_ids : (string * string) list = external_ids
  method discs : disc list = discs
end

class artist id link name portrait biographies similar_artists genres years_active albums = object
  method id : id = id
  method link : link = link
  method name : string = name
  method portrait : portrait option = portrait
  method biographies : biography list = biographies
  method similar_artists : similar_artist list = similar_artists
  method genres : string list = genres
  method years_active : int list = years_active
  method albums : id list = albums
end

class artist_search id link name portrait popularity restrictions = object
  method id : id = id
  method link : link = link
  method name : string = name
  method portrait : portrait option = portrait
  method popularity : float = popularity
  method restrictions : restriction list = restrictions
end

class album_search id link name artist artist_id cover popularity restrictions external_ids = object
  method id : id = id
  method link : link = link
  method name : string = name
  method artist : string = artist
  method artist_id : id = artist_id
  method cover : id option = cover
  method popularity : float = popularity
  method restrictions : restriction list = restrictions
  method external_ids : (string * string) list = external_ids
end

class simple_search_result link did_you_mean total_artists total_albums total_tracks = object
  method link : link = link
  method did_you_mean : string option = did_you_mean
  method total_artists : int = total_artists
  method total_albums : int = total_albums
  method total_tracks : int = total_tracks
end

class search_result link did_you_mean total_artists total_albums total_tracks artists albums tracks = object
  method link : link = link
  method did_you_mean : string option = did_you_mean
  method total_artists : int = total_artists
  method total_albums : int = total_albums
  method total_tracks : int = total_tracks
  method artists : artist_search list = artists
  method albums : album_search list = albums
  method tracks : track list = tracks
end

class playlist id link name user time revision checksum collaborative destroyed tracks = object
  method id : id = id
  method link : link = link
  method name : string = name
  method user : string = user
  method time : float = time
  method revision : int = revision
  method checksum : int = checksum
  method collaborative : bool = collaborative
  method destroyed : bool = destroyed
  method tracks : id list = tracks
end

class meta_playlist user time revision checksum collaborative playlists = object
  method user : string = user
  method time : float = time
  method revision : int = revision
  method checksum : int = checksum
  method collaborative : bool = collaborative
  method playlists : id list = playlists
end

(* +-----------------------------------------------------------------+
   | Writers                                                         |
   +-----------------------------------------------------------------+ *)

let write_portrait oc (portrait : portrait) =
  XML.write_node oc "portrait" []
    (fun oc ->
       lwt () = XML.write_data oc "id" (string_of_id portrait#id) in
       lwt () = XML.write_data oc "width" (string_of_int portrait#width) in
       lwt () = XML.write_data oc "height" (string_of_int portrait#height) in
       return ())

let write_biography oc (bio : biography) =
  XML.write_node oc "bio" []
    (fun oc ->
       lwt () = XML.write_data oc "text" bio#text in
       lwt () = XML.write_nodes oc "portraits" write_portrait bio#portraits in
       return ())

let write_restriction oc (restriction : restriction) =
  let attrs = [] in
  let attrs = match restriction#allowed with Some l -> ("allowed", String.concat "," l) :: attrs | None -> attrs in
  let attrs = match restriction#forbidden with Some l -> ("forbidden", String.concat "," l) :: attrs | None -> attrs in
  let attrs = match restriction#catalogues with Some l -> ("catalogues", String.concat "," l) :: attrs | None -> attrs in
  XML.write_empty_node oc "restriction" attrs

let write_similar_artist oc (similar_artist : similar_artist) =
  XML.write_node oc "similar-artist" []
    (fun oc ->
       lwt () = XML.write_data oc "id" (string_of_id similar_artist#id) in
       lwt () = XML.write_data oc "name" similar_artist#name in
       lwt () = match similar_artist#portrait with Some x -> XML.write_data oc "portrait" (string_of_id x) | None -> return () in
       lwt () = XML.write_data oc "genres" (String.concat "," similar_artist#genres) in
       lwt () = XML.write_data oc "years-active" (String.concat "," (List.map string_of_int similar_artist#years_active)) in
       lwt () = XML.write_nodes oc "restrictions" write_restriction similar_artist#restrictions in
       return ())

let write_file oc file =
  XML.write_empty_node oc "file" [("id", string_of_id file#id); ("format", Printf.sprintf "%s,%d" file#format file#bitrate)]

let write_alternative oc (alternative : alternative) =
  XML.write_node oc "track" []
    (fun oc ->
       lwt () = XML.write_data oc "id" (string_of_id alternative#id) in
       lwt () = XML.write_nodes oc "files" write_file alternative#files in
       lwt () = XML.write_nodes oc "restrictions" write_restriction alternative#restrictions in
       return ())

let write_external_id oc (t, i) =
  XML.write_empty_node oc "external-id" [("type", t); ("id", i)]

let write_track oc (track : track) =
  XML.write_node oc "track" []
    (fun oc ->
       lwt () = XML.write_data oc "mlspot-cache-version" (string_of_int track_cache_version) in
       lwt () = XML.write_data oc "id" (string_of_id track#id) in
       lwt () = XML.write_data oc "title" track#title in
       lwt () = XML.write_data oc "explicit" (string_of_bool track#explicit) in
       lwt () = Lwt_list.iter_s (XML.write_data oc "artist") track#artists in
       lwt () = Lwt_list.iter_s (XML.write_data oc "artist-id") (List.map string_of_id track#artists_id) in
       lwt () = XML.write_data oc "album" track#album in
       lwt () = XML.write_data oc "album-id" (string_of_id track#album_id) in
       lwt () = XML.write_data oc "album-artist" track#album_artist in
       lwt () = XML.write_data oc "album-artist-id" (string_of_id track#album_artist_id) in
       lwt () = XML.write_data oc "track-number" (string_of_int track#number) in
       lwt () = XML.write_data oc "length" (string_of_int (int_of_float (track#length *. 1000.))) in
       lwt () = XML.write_nodes oc "files" write_file track#files in
       lwt () = match track#cover with Some id -> XML.write_data oc "cover" (string_of_id id) | None -> return () in
       lwt () = XML.write_data oc "year" (string_of_int track#year) in
       lwt () = XML.write_data oc "popularity" (string_of_float track#popularity) in
       lwt () = XML.write_nodes oc "external-ids" write_external_id track#external_ids in
       lwt () = XML.write_nodes oc "alternatives" write_alternative track#alternatives in
       return ())

let write_track_id oc id =
  XML.write_data oc "track" (string_of_id id)

let write_disc oc disc =
  XML.write_node oc "disc" []
    (fun oc ->
       lwt () = XML.write_data oc "disc-number" (string_of_int disc#number) in
       lwt () = match disc#name with Some name -> XML.write_data oc "name" name | None -> return () in
       lwt () = Lwt_list.iter_s (write_track_id oc) disc#tracks in
       return ())

let write_copyright oc (kind, data) =
  XML.write_data oc kind data

let write_album oc (album : album) =
  XML.write_node oc "album" []
    (fun oc ->
       lwt () = XML.write_data oc "mlspot-cache-version" (string_of_int album_cache_version) in
       lwt () = XML.write_data oc "id" (string_of_id album#id) in
       lwt () = XML.write_data oc "name" album#name in
       lwt () = XML.write_data oc "artist" album#artist in
       lwt () = XML.write_data oc "artist-id" (string_of_id album#artist_id) in
       lwt () = XML.write_data oc "album-type" album#album_type in
       lwt () = XML.write_data oc "year" (string_of_int album#year) in
       lwt () = match album#cover with Some id -> XML.write_data oc "cover" (string_of_id id) | None -> return () in
       lwt () = XML.write_nodes oc "copyright" write_copyright album#copyrights in
       lwt () = XML.write_nodes oc "restrictions" write_restriction album#restrictions in
       lwt () = XML.write_nodes oc "external-ids" write_external_id album#external_ids in
       lwt () = XML.write_nodes oc "discs" write_disc album#discs in
       return ())

let write_album_id oc id =
  XML.write_data oc "album" (string_of_id id)

let write_artist oc (artist : artist) =
  XML.write_node oc "artist" []
    (fun oc ->
       lwt () = XML.write_data oc "mlspot-cache-version" (string_of_int artist_cache_version) in
       lwt () = XML.write_data oc "id" (string_of_id artist#id) in
       lwt () = XML.write_data oc "name" artist#name in
       lwt () = match artist#portrait with Some portrait -> write_portrait oc portrait | None -> return () in
       lwt () = XML.write_nodes oc "bios" write_biography artist#biographies in
       lwt () = XML.write_nodes oc "similar-artists" write_similar_artist artist#similar_artists in
       lwt () = XML.write_data oc "genres" (String.concat "," artist#genres) in
       lwt () = XML.write_data oc "years-active" (String.concat "," (List.map string_of_int artist#years_active)) in
       lwt () = XML.write_nodes oc "albums" write_album_id artist#albums in
       return ())

(* +-----------------------------------------------------------------+
   | Commands                                                        |
   +-----------------------------------------------------------------+ *)

type command =
  | CMD_SECRET_BLOCK
  | CMD_PING
  | CMD_GET_DATA
  | CMD_CHANNEL_DATA
  | CMD_CHANNEL_ERROR
  | CMD_CHANNEL_ABORT
  | CMD_REQUEST_KEY
  | CMD_AES_KEY
  | CMD_AES_KEY_ERROR
  | CMD_CACHE_HASH
  | CMD_SHA_HASH
  | CMD_IMAGE
  | CMD_COUNTRY_CODE
  | CMD_P2P_SETUP
  | CMD_P2P_INIT_BLOCK
  | CMD_BROWSE
  | CMD_SEARCH
  | CMD_PLAYLIST_CHANGED
  | CMD_GET_DATA_PLAYLIST
  | CMD_CHANGE_PLAYLIST
  | CMD_NOTIFY
  | CMD_LOG
  | CMD_PONG
  | CMD_PONG_ACK
  | CMD_PAUSE
  | CMD_REQUEST_AD
  | CMD_REQUEST_PLAY
  | CMD_PROD_INFO
  | CMD_WELCOME

exception Unknown_command of int

let string_of_command = function
  | CMD_SECRET_BLOCK -> "CMD_SECRET_BLOCK"
  | CMD_PING -> "CMD_PING"
  | CMD_GET_DATA -> "CMD_GET_DATA"
  | CMD_CHANNEL_DATA -> "CMD_CHANNEL_DATA"
  | CMD_CHANNEL_ERROR -> "CMD_CHANNEL_ERROR"
  | CMD_CHANNEL_ABORT -> "CMD_CHANNEL_ABORT"
  | CMD_REQUEST_KEY -> "CMD_REQUEST_KEY"
  | CMD_AES_KEY -> "CMD_AES_KEY"
  | CMD_AES_KEY_ERROR -> "CMD_AES_KEY_ERROR"
  | CMD_CACHE_HASH -> "CMD_CACHE_HASH"
  | CMD_SHA_HASH -> "CMD_SHA_HASH"
  | CMD_IMAGE -> "CMD_IMAGE"
  | CMD_COUNTRY_CODE -> "CMD_COUNTRY_CODE"
  | CMD_P2P_SETUP -> "CMD_P2P_SETUP"
  | CMD_P2P_INIT_BLOCK -> "CMD_P2P_INIT_BLOCK"
  | CMD_BROWSE -> "CMD_BROWSE"
  | CMD_SEARCH -> "CMD_SEARCH"
  | CMD_PLAYLIST_CHANGED -> "CMD_PLAYLIST_CHANGED"
  | CMD_GET_DATA_PLAYLIST -> "CMD_GET_DATA_PLAYLIST"
  | CMD_CHANGE_PLAYLIST -> "CMD_CHANGE_PLAYLIST"
  | CMD_NOTIFY -> "CMD_NOTIFY"
  | CMD_LOG -> "CMD_LOG"
  | CMD_PONG -> "CMD_PONG"
  | CMD_PONG_ACK -> "CMD_PONG_ACK"
  | CMD_PAUSE -> "CMD_PAUSE"
  | CMD_REQUEST_AD -> "CMD_REQUEST_AD"
  | CMD_REQUEST_PLAY -> "CMD_REQUEST_PLAY"
  | CMD_PROD_INFO -> "CMD_PROD_INFO"
  | CMD_WELCOME -> "CMD_WELCOME"

let command_of_int = function
  | 0x02 -> CMD_SECRET_BLOCK
  | 0x04 -> CMD_PING
  | 0x08 -> CMD_GET_DATA
  | 0x09 -> CMD_CHANNEL_DATA
  | 0x0a -> CMD_CHANNEL_ERROR
  | 0x0b -> CMD_CHANNEL_ABORT
  | 0x0c -> CMD_REQUEST_KEY
  | 0x0d -> CMD_AES_KEY
  | 0x0e -> CMD_AES_KEY_ERROR
  | 0x0f -> CMD_CACHE_HASH
  | 0x10 -> CMD_SHA_HASH
  | 0x19 -> CMD_IMAGE
  | 0x1b -> CMD_COUNTRY_CODE
  | 0x20 -> CMD_P2P_SETUP
  | 0x21 -> CMD_P2P_INIT_BLOCK
  | 0x30 -> CMD_BROWSE
  | 0x31 -> CMD_SEARCH
  | 0x34 -> CMD_PLAYLIST_CHANGED
  | 0x35 -> CMD_GET_DATA_PLAYLIST
  | 0x36 -> CMD_CHANGE_PLAYLIST
  | 0x42 -> CMD_NOTIFY
  | 0x48 -> CMD_LOG
  | 0x49 -> CMD_PONG
  | 0x4a -> CMD_PONG_ACK
  | 0x4b -> CMD_PAUSE
  | 0x4e -> CMD_REQUEST_AD
  | 0x4f -> CMD_REQUEST_PLAY
  | 0x50 -> CMD_PROD_INFO
  | 0x69 -> CMD_WELCOME
  | cmd -> raise (Unknown_command cmd)

let int_of_command = function
  | CMD_SECRET_BLOCK -> 0x02
  | CMD_PING -> 0x04
  | CMD_GET_DATA -> 0x08
  | CMD_CHANNEL_DATA -> 0x09
  | CMD_CHANNEL_ERROR -> 0x0a
  | CMD_CHANNEL_ABORT -> 0x0b
  | CMD_REQUEST_KEY -> 0x0c
  | CMD_AES_KEY -> 0x0d
  | CMD_AES_KEY_ERROR -> 0x0e
  | CMD_CACHE_HASH -> 0x0f
  | CMD_SHA_HASH -> 0x10
  | CMD_IMAGE -> 0x19
  | CMD_COUNTRY_CODE -> 0x1b
  | CMD_P2P_SETUP -> 0x20
  | CMD_P2P_INIT_BLOCK -> 0x21
  | CMD_BROWSE -> 0x30
  | CMD_SEARCH -> 0x31
  | CMD_PLAYLIST_CHANGED -> 0x34
  | CMD_GET_DATA_PLAYLIST -> 0x35
  | CMD_CHANGE_PLAYLIST -> 0x36
  | CMD_NOTIFY -> 0x42
  | CMD_LOG -> 0x48
  | CMD_PONG -> 0x49
  | CMD_PONG_ACK -> 0x4a
  | CMD_PAUSE -> 0x4b
  | CMD_REQUEST_AD -> 0x4e
  | CMD_REQUEST_PLAY -> 0x4f
  | CMD_PROD_INFO -> 0x50
  | CMD_WELCOME -> 0x69

(* +-----------------------------------------------------------------+
   | Session                                                         |
   +-----------------------------------------------------------------+ *)

module Channel_map = Map.Make (struct type t = int let compare a b = a - b end)

type channel = {
  ch_data : (string * int * int) Queue.t;
  (* Queue of pending data. *)

  mutable ch_done : bool;
  (* Whether the channel is terminated. *)

  ch_cond : unit Lwt_condition.t;
  (* Condition used to notify thread waiting for data. *)

  ch_error_waiter : unit Lwt.t;
  ch_error_wakener : unit Lwt.u;
  (* Thread wakeup when an error occurs on the channel. *)
}

(* Parameters for online sessions. *)
type session_parameters = {
  mutable disconnected : bool;
  (* Whether the session has been disconnected. In this case [socket]
     has been closed and [logout_wakener] has been wakeup. *)

  socket : Lwt_unix.file_descr;
  (* The socket used to communicate with the server. *)

  ic : Lwt_io.input_channel;
  (* The input channel used to receive data from the server. *)

  oc : Lwt_io.output_channel;
  (* The output channel used to send data to the server. *)

  shn_send : shn_ctx;
  (* The shn context for sending packets. *)

  shn_recv : shn_ctx;
  (* The shn context for receiving packets. *)

  mutable send_iv : int;
  (* Send init vector. *)

  mutable recv_iv : int;
  (* Recv init vector. *)

  mutable channels : channel Channel_map.t;
  (* Used channels. *)

  mutable next_channel_id : int;
  (* ID the next maybe available channel. This is used to speed up the
     search. *)

  channel_released : unit Lwt_condition.t;
  (* Condition signaled when a channel becomes available. *)

  login_waiter : unit Lwt.t;
  login_wakener : unit Lwt.u;
  (* Thread wakeuped when the login is complete. *)

  logout_waiter : unit Lwt.t;
  logout_wakener : unit Lwt.u;
  (* Thread waiting for the session to be closed. *)

  set_products : product list -> unit;
  (* Set products informations. *)

  set_country_code : string -> unit;
  (* Set the country code. *)

  playlists : ((unit -> unit) * playlist signal Weak.t) ID_table.t;
  (* All playlist in use. *)

  mutable meta_playlist : ((unit -> unit) * meta_playlist signal Weak.t) option;
  (* The list of all playlists. *)
}

(* Sessions are objects so they are comparable and hashable. *)
class session ?(use_cache = true) ?(cache_dir = Filename.concat xdg_cache_home "mlspot") () =
  let products, set_products = S.create ([] : product list) in
  let country_code, set_country_code = S.create "" in
object
  val mutable session_parameters : session_parameters option = None
    (* Session parameters for online sessions. *)

  method session_parameters =
    match session_parameters with
      | Some sp -> sp
      | None -> raise Offline

  method get_session_parameters = session_parameters
  method set_session_parameters sp = session_parameters <- sp

  val mutable config_use_cache = use_cache
  method get_use_cache = config_use_cache
  method set_use_cache x = config_use_cache <- x

  val mutable config_cache_dir = cache_dir
  method get_cache_dir = config_cache_dir
  method set_cache_dir x = config_cache_dir <- x

  method products = products
  method set_products = set_products

  method country_code = country_code
  method set_country_code = set_country_code
end

let create ?use_cache ?cache_dir () = new session ?use_cache ?cache_dir ()
let online session = session#get_session_parameters <> None

let get_use_cache session = session#get_use_cache
let set_use_cache session x = session#set_use_cache x

let get_cache_dir session = session#get_cache_dir
let set_cache_dir session x = session#set_cache_dir x

let products session = session#products

let country_code session = session#country_code

(* +-----------------------------------------------------------------+
   | Utils                                                           |
   +-----------------------------------------------------------------+ *)

(* Return a thread which fails when the session is closed. *)
let or_logout sp w =
  pick [w; sp.logout_waiter >> fail Exit]

(* +-----------------------------------------------------------------+
   | Channels                                                        |
   +-----------------------------------------------------------------+ *)

class steam_of_channel header channel = object(self)
  val mutable state = if header then BOS else BODY

  val mutable buffer = ""
  val mutable offset = 0
  val mutable length = 0

  method private read_byte =
    if length > 0 then begin
      let byte = Char.code buffer.[offset] in
      offset <- offset + 1;
      length <- length - 1;
      return byte
    end else begin
      match_lwt self#fetch with
        | true ->
            self#read_byte
        | false ->
            raise_lwt (Error "premature end of channel")
    end

  method private skip_bytes skip_len =
    if skip_len < length then begin
      offset <- offset + skip_len;
      length <- length - skip_len;
      return ()
    end else begin
      let skip_len = skip_len - length in
      match_lwt self#fetch with
        | true ->
            self#skip_bytes skip_len
        | false ->
            raise_lwt (Error "premature end of channel")
    end

  method private read_int16 =
    lwt x0 = self#read_byte in
    lwt x1 = self#read_byte in
    return ((x0 lsl 8) lor x1)

  method private fetch =
    if not (Queue.is_empty channel.ch_data) then begin
      let buf, ofs, len = Queue.take channel.ch_data in
      buffer <- buf;
      offset <- ofs;
      length <- len;
      return true
    end else if channel.ch_done then begin
      state <- EOS;
      return false
    end else begin
      lwt () = pick [channel.ch_error_waiter; Lwt_condition.wait channel.ch_cond] in
      self#fetch
    end

  method private skip_headers =
    lwt len = self#read_int16 in
    if len = 0 then
      return ()
    else begin
      lwt () = self#skip_bytes len in
      self#skip_headers
    end

  method read buf ofs len =
    match state with
      | BOS ->
          lwt () = self#skip_headers in
          state <- BODY;
          self#read buf ofs len
      | EOS ->
          return 0
      | BODY ->
          if length = 0 then
            match_lwt self#fetch with
              | true ->
                  self#read buf ofs len
              | false ->
                  return 0
          else begin
            let len = min len length in
            String.blit buffer offset buf ofs len;
            offset <- offset + len;
            length <- length - len;
            return len
          end

  method close =
    return ()
end

(* Allocate a channel. If all channel ID are taken, wait for one to be
   released. It returns the channel id, the packet of the channel and
   a thread which terminates when the packet is completely
   received. *)
let alloc_channel ?(header = true) sp =
  let rec loop id =
    if Channel_map.mem id sp.channels then
      let id = (id + 1) land 0xffff in
      if id = sp.next_channel_id then
        None
      else
        loop id
    else begin
      sp.next_channel_id <- id + 1;
      let waiter, wakener = wait () in
      let channel = {
        ch_data = Queue.create ();
        ch_cond = Lwt_condition.create ();
        ch_done = false;
        ch_error_waiter = or_logout sp waiter;
        ch_error_wakener = wakener;
      } in
      sp.channels <- Channel_map.add id channel sp.channels;
      Some (id, new steam_of_channel header channel)
    end;
  in
  let rec wait_for_id () =
    match loop sp.next_channel_id with
      | Some x ->
          return x
      | None ->
          (* Wait for either a channel to be released, or the session
             to be closed. *)
          lwt () = or_logout sp (Lwt_condition.wait sp.channel_released) in
          wait_for_id ()
  in
  wait_for_id ()

(* Make one channel available again. *)
let release_channel sp id =
  sp.channels <- Channel_map.remove id sp.channels;
  (* Wakeup at most one thread waiting for an id. *)
  Lwt_condition.signal sp.channel_released ()

(* +-----------------------------------------------------------------+
   | Packets                                                         |
   +-----------------------------------------------------------------+ *)

(* Send a packet with the given command using the given session
   parameters. *)
let send_packet sp command packet =
  let nonce = String.create 4 in
  let iv = sp.send_iv in
  sp.send_iv <- iv + 1;
  put_int32 nonce 0 iv;
  shn_nonce sp.shn_send nonce 0 4;
  let len = String.length packet in
  let buffer = String.create (3 + len + 4) in
  put_int8 buffer 0 (int_of_command command);
  put_int16 buffer 1 len;
  String.unsafe_blit packet 0 buffer 3 len;
  shn_encrypt sp.shn_send buffer 0 (3 + len);
  shn_finish sp.shn_send buffer (3 + len) 4;
  or_logout sp (Lwt_io.write_from_exactly sp.oc buffer 0 (3 + len + 4))

(* Read one packet using the given session parameters.

   Warning: this function is not thread-safe and must be always
   invoked from the same thread, i.e. the dispatcher thread. *)
let recv_packet sp =
  let nonce = String.create 4 in
  let iv = sp.recv_iv in
  sp.recv_iv <- iv + 1;
  put_int32 nonce 0 iv;
  shn_nonce sp.shn_recv nonce 0 4;
  let header = String.create 3 in
  lwt () = or_logout sp (Lwt_io.read_into_exactly sp.ic header 0 3) in
  shn_decrypt sp.shn_recv header 0 3;
  let len = get_int16 header 1 in
  let packet_len = len + 4 in
  let payload = String.create packet_len in
  lwt () = or_logout sp (Lwt_io.read_into_exactly sp.ic payload 0 packet_len) in
  shn_decrypt sp.shn_recv payload 0 packet_len;
  let payload = String.sub payload 0 len in
  try
    let command = command_of_int (Char.code (String.unsafe_get header 0)) in
    return (command, payload)
  with Unknown_command cmd as exn ->
    if debug then delay (save_raw_data (Printf.sprintf "unknown command 0x%02x" cmd) payload);
    raise_lwt exn

(* +-----------------------------------------------------------------+
   | Cache                                                           |
   +-----------------------------------------------------------------+ *)

module Cache = struct
  let released = Lwt_condition.create ()

  (* Allow only 128 threads to use the cache at the same time. *)
  let available = ref 128

  (* How much time to sleep in case of too many open files. *)
  let too_many_open_files_delay = 0.1

  let rec acquire () =
    if !available > 0 then begin
      decr available;
      return ()
    end else
      lwt () = Lwt_condition.wait released in
      acquire ()

  let release () =
    incr available;
    Lwt_condition.signal released ()

  let with_lock f =
    lwt () = acquire () in
    try_lwt
      f ()
    finally
      release ();
      return ()

  let contains session name =
    if session#get_use_cache then
      Sys.file_exists (Filename.concat session#get_cache_dir name)
    else
      false

  let rec load session name =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      try_lwt
        with_lock (fun () -> Lwt_io.with_file ~mode:Lwt_io.input filename Lwt_io.read >|= fun x -> Some x)
      with
        | Unix.Unix_error (Unix.EMFILE, _, _) ->
            lwt () = Lwt_unix.sleep too_many_open_files_delay in
            load session name
        | Unix.Unix_error (error, _, _) ->
            ignore (Lwt_log.debug_f ~section "failed to load %S from the cache: %s" name (Unix.error_message error));
            return None
    else
      return None

  let rec save session name data =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      if Sys.file_exists filename then
        return ()
      else
        try_lwt
          lwt ok = mkdir_p (Filename.dirname filename) in
          if ok then
            with_lock (fun () -> Lwt_io.with_file ~mode:Lwt_io.output filename (fun oc -> Lwt_io.write oc data))
          else
            return ()
        with
          | Unix.Unix_error (Unix.EMFILE, _, _) ->
              lwt () = Lwt_unix.sleep too_many_open_files_delay in
              save session name data
          | Unix.Unix_error (error, _, _) ->
              ignore (Lwt_log.error_f ~section "failed to save %S to the cache: %s" name (Unix.error_message error));
              return ()
    else
      return ()

  let rec get_reader session name =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      lwt () = acquire () in
      try_lwt
        lwt fd = Lwt_unix.openfile filename [Unix.O_RDONLY] 0 in
        return (Some (filename, fd))
      with
        | Unix.Unix_error (Unix.ENOENT, _, _) ->
            release ();
            return None
        | Unix.Unix_error (Unix.EMFILE, _, _) ->
            release ();
            lwt () = Lwt_unix.sleep too_many_open_files_delay in
            get_reader session name
        | Unix.Unix_error (error, _, _) ->
            release ();
            ignore (Lwt_log.error_f ~section "failed to open %S from the cache for reading: %s" name (Unix.error_message error));
            return None
    else
      return None

  let rec get_writer session ?(overwrite = false) name =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      if not overwrite && Sys.file_exists filename then
        return None
      else
        lwt () = acquire () in
        try_lwt
          lwt ok = mkdir_p (Filename.dirname filename) in
          if ok then
            lwt fd = Lwt_unix.openfile filename [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o644 in
            return (Some (filename, fd))
          else
            return None
        with
          | Unix.Unix_error (Unix.EMFILE, _, _) ->
              release ();
              lwt () = Lwt_unix.sleep too_many_open_files_delay in
              get_writer session name
          | Unix.Unix_error (error, _, _) ->
              release ();
              ignore (Lwt_log.error_f "failed to open %S from the cache for writing: %s" name (Unix.error_message error));
              return None
    else
      return None
end

(* +-----------------------------------------------------------------+
   | Saving to the cache                                             |
   +-----------------------------------------------------------------+ *)

let save_track session track =
  match_lwt Cache.get_writer session (make_filename ["metadata"; "tracks"; (string_of_id track#id ^ ".xml.gz")]) with
    | None ->
        return ()
    | Some (file, fd) ->
        let oc = XML.create_writer fd file in
        try_lwt
          lwt () = write_track oc track in
          XML.close oc
        with exn ->
          ignore (Lwt_log.error_f ~section ~exn "failed to save metadata for track %S" (string_of_id track#id));
          XML.abort oc
        finally
          Cache.release ();
          return ()

let save_album session album =
  match_lwt Cache.get_writer session (make_filename ["metadata"; "albums"; (string_of_id album#id ^ ".xml.gz")]) with
    | None ->
        return ()
    | Some (file, fd) ->
        let oc = XML.create_writer fd file in
        try_lwt
          lwt () = write_album oc album in
          XML.close oc
        with exn ->
          ignore (Lwt_log.error_f ~section ~exn "failed to save metadata for album %S" (string_of_id album#id));
          XML.abort oc
        finally
          Cache.release ();
          return ()

let save_artist session artist =
  match_lwt Cache.get_writer session (make_filename ["metadata"; "artists"; (string_of_id artist#id ^ ".xml.gz")]) with
    | None ->
        return ()
    | Some (file, fd) ->
        let oc = XML.create_writer fd file in
        try_lwt
          lwt () = write_artist oc artist in
          XML.close oc
        with exn ->
          ignore (Lwt_log.error_f ~section ~exn "failed to save metadata for artist %S" (string_of_id artist#id));
          XML.abort oc
        finally
          Cache.release ();
          return ()

(* +-----------------------------------------------------------------+
   | Parsers                                                         |
   +-----------------------------------------------------------------+ *)

let dummy_id = ID.of_bytes ""

let portraits = Weak_cache.create 1024

class portrait_parser assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable width = 0
  val mutable height = 0
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get portraits id)
        | "width" -> new data (fun x -> width <- int_of_string x)
        | "height" -> new data (fun x -> height <- int_of_string x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    match cached with
      | Some x ->
          assign x
      | None ->
          assign (Weak_cache.find portraits id (fun () -> new portrait id (Image id) width height))
end

class biography_parser assign = object
  inherit xml_parser

  val mutable text = ""
  val mutable portraits = []

  method node tag attrs =
    match tag with
      | "text" -> new data (fun x -> text <- x)
      | "portraits" -> new nodes "portrait" (fun attrs assign -> new portrait_parser assign) (fun x -> portraits <- x)
      | _ -> new xml_parser

  method stop =
    assign (new biography text portraits)
end

let parse_restriction attrs assign =
  let catalogues = ref None in
  let forbidden = ref None in
  let allowed = ref None in
  List.iter
    (function
       | ("catalogues", x) -> catalogues := Some (split ',' x)
       | ("forbidden", x) -> forbidden := Some (split_space_coma x)
       | ("allowed", x) -> allowed := Some (split_space_coma x)
       | _ -> ())
    attrs;
  assign (new restriction !catalogues !forbidden !allowed);
  new xml_parser

let similar_artists = Weak_cache.create 1024

class similar_artist_parser assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable name = ""
  val mutable portrait = None
  val mutable genres = []
  val mutable years_active = []
  val mutable restrictions = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get similar_artists id)
        | "name" -> new data (fun x -> name <- x)
        | "portrait" -> new data (fun x -> portrait <- Some (id_of_string x))
        | "genres" -> new data (fun x -> genres <- split ',' x)
        | "years-active" -> new data (fun x -> years_active <- List.map int_of_string (split ',' x))
        | "restrictions" -> new nodes "restriction" parse_restriction (fun x -> restrictions <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    match cached with
      | Some x ->
          assign x
      | None ->
          assign (Weak_cache.find similar_artists id (fun () -> new similar_artist id (Artist id) name portrait genres years_active restrictions))
end

let files = Weak_cache.create 1024

let parse_file attrs assign =
  let id = ref dummy_id in
  let format = ref "" in
  let bitrate = ref 0 in
  List.iter
    (function
       | ("id", x) -> id := id_of_string x
       | ("format", x) -> begin
           match split ',' x with
             | x :: y :: _ ->
                 format := x;
                 bitrate := int_of_string y
             | _ ->
                 failwith "invalid file format"
         end
       | _ -> ())
    attrs;
  assign (Weak_cache.find files !id (fun () -> new file !id !format !bitrate));
  new xml_parser

let alternatives = Weak_cache.create 1024

class alternative_parser assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable files = []
  val mutable restrictions = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get alternatives id)
        | "files" -> new nodes "file" parse_file (fun x -> files <- x)
        | "restrictions" -> new nodes "restriction" parse_restriction (fun x -> restrictions <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    match cached with
      | Some x ->
          assign x
      | None ->
          assign (Weak_cache.find alternatives id (fun () -> new alternative id (Track id) files restrictions))
end

let parse_external_id attrs assign =
  let typ = ref "" in
  let id = ref "" in
  List.iter
    (function
       | ("type", x) -> typ := x
       | ("id", x) -> id := x
       | _ -> ())
    attrs;
  assign (!typ, !id);
  new xml_parser

let tracks = Weak_cache.create 1024

class track_parser session assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable title = ""
  val mutable explicit = false
  val mutable artists = []
  val mutable artists_id = []
  val mutable album = ""
  val mutable album_id = dummy_id
  val mutable album_artist = ""
  val mutable album_artist_id = dummy_id
  val mutable year = 0
  val mutable number = 0
  val mutable length = 0.
  val mutable files = []
  val mutable cover = None
  val mutable popularity = 0.
  val mutable external_ids = []
  val mutable alternatives = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "mlspot-cache-version" -> new data (fun x -> if int_of_string x <> track_cache_version then raise Cache_version_mismatch)
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get tracks id)
        | "title" -> new data (fun x -> title <- x)
        | "explicit" -> new data (fun x -> explicit <- bool_of_string x)
        | "artist" -> new data (fun x -> artists <- x :: artists)
        | "artist-id" -> new data (fun x -> artists_id <- id_of_string x :: artists_id)
        | "album" -> new data (fun x -> album <- x)
        | "album-id" -> new data (fun x -> album_id <- id_of_string x)
        | "album-artist" -> new data (fun x -> album_artist <- x)
        | "album-artist-id" -> new data (fun x -> album_artist_id <- id_of_string x)
        | "year" -> new data (fun x -> year <- int_of_string x)
        | "track-number" -> new data (fun x -> number <- int_of_string x)
        | "length" -> new data (fun x -> length <- float_of_string x /. 1000.)
        | "files" -> new nodes "file" parse_file (fun x -> files <- x)
        | "cover" -> new data (fun x -> cover <- Some (id_of_string x))
        | "popularity" -> new data (fun x -> popularity <- float_of_string x)
        | "external-ids" -> new nodes "external-id" parse_external_id (fun x -> external_ids <- x)
        | "alternatives" -> new nodes "track" (fun attrs assign -> new alternative_parser assign) (fun x -> alternatives <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    let track =
      match cached with
        | Some track ->
            track
        | None ->
            Weak_cache.find tracks id (fun () -> new track id (Track id) title explicit (List.rev artists) (List.rev artists_id) album album_id album_artist album_artist_id year number length files cover popularity external_ids alternatives)
    in
    delay (save_track session track);
    assign track
end

class track_parser_from_album session album album_id album_artist album_artist_id year cover assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable title = ""
  val mutable explicit = false
  val mutable artists = []
  val mutable artists_id = []
  val mutable number = 0
  val mutable length = 0.
  val mutable files = []
  val mutable popularity = 0.
  val mutable external_ids = []
  val mutable alternatives = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get tracks id)
        | "title" -> new data (fun x -> title <- x)
        | "explicit" -> new data (fun x -> explicit <- bool_of_string x)
        | "artist" -> new data (fun x -> artists <- x :: artists)
        | "artist-id" -> new data (fun x -> artists_id <- id_of_string x :: artists_id)
        | "track-number" -> new data (fun x -> number <- int_of_string x)
        | "length" -> new data (fun x -> length <- float_of_string x /. 1000.)
        | "files" -> new nodes "file" parse_file (fun x -> files <- x)
        | "popularity" -> new data (fun x -> popularity <- float_of_string x)
        | "external-ids" -> new nodes "external-id" parse_external_id (fun x -> external_ids <- x)
        | "alternatives" -> new nodes "track" (fun attrs assign -> new alternative_parser assign) (fun x -> alternatives <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    let track =
      match cached with
        | Some track ->
            track
        | None ->
            Weak_cache.find tracks id (fun () -> new track id (Track id) title explicit (List.rev artists) (List.rev artists_id) album album_id album_artist album_artist_id year number length files cover popularity external_ids alternatives)
    in
    delay (save_track session track);
    assign id
end

class disc_parser session album album_id album_artist album_artist_id year cover assign = object
  inherit xml_parser

  val mutable number = 0
  val mutable name = None
  val mutable tracks = []

  method node tag attrs =
    match tag with
      | "disc-number" -> new data (fun x -> number <- int_of_string x)
      | "name" -> new data (fun x -> name <- Some x)
      | "track" -> new track_parser_from_album session album album_id album_artist album_artist_id year cover (fun id -> tracks <- id :: tracks)
      | _ -> new xml_parser

  method stop =
    assign (new disc number name (List.rev tracks))
end

class disc_parser_from_cache assign = object
  inherit xml_parser

  val mutable number = 0
  val mutable name = None
  val mutable tracks = []

  method node tag attrs =
    match tag with
      | "disc-number" -> new data (fun x -> number <- int_of_string x)
      | "name" -> new data (fun x -> name <- Some x)
      | "track" -> new data (fun x -> tracks <- id_of_string x :: tracks)
      | _ -> new xml_parser

  method stop =
    assign (new disc number name (List.rev tracks))
end

class copyrights_parser assign = object
  inherit xml_parser

  val mutable copyrights = []

  method node tag attrs =
    new data (fun x -> copyrights <- (tag, x) :: copyrights)

  method stop =
    assign (List.rev copyrights)
end

let albums = Weak_cache.create 1024

class album_parser session assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable name = ""
  val mutable artist = ""
  val mutable artist_id = dummy_id
  val mutable album_type = ""
  val mutable year = 0
  val mutable cover = None
  val mutable copyrights = []
  val mutable restrictions = []
  val mutable external_ids = []
  val mutable discs = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get albums id)
        | "name" -> new data (fun x -> name <- x)
        | "artist" -> new data (fun x -> artist <- x)
        | "artist-id" -> new data (fun x -> artist_id <- id_of_string x)
        | "album-type" -> new data (fun x -> album_type <- x)
        | "year" -> new data (fun x -> year <- int_of_string x)
        | "cover" -> new data (fun x -> cover <- Some (id_of_string x))
        | "copyright" -> new copyrights_parser (fun x -> copyrights <- x)
        | "restrictions" -> new nodes "restriction" parse_restriction (fun x -> restrictions <- x)
        | "external-ids" -> new nodes "external-id" parse_external_id (fun x -> external_ids <- x)
        | "discs" -> new nodes "disc" (fun attrs assign -> new disc_parser session name id artist artist_id year cover assign) (fun x -> discs <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    let album =
      match cached with
        | Some album ->
            album
        | None ->
            Weak_cache.find albums id (fun () -> new album id (Album id) name artist artist_id album_type year cover copyrights restrictions external_ids discs)
    in
    delay (save_album session album);
    assign album
end

class album_parser_from_cache assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable name = ""
  val mutable artist = ""
  val mutable artist_id = dummy_id
  val mutable album_type = ""
  val mutable year = 0
  val mutable cover = None
  val mutable copyrights = []
  val mutable restrictions = []
  val mutable external_ids = []
  val mutable discs = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "mlspot-cache-version" -> new data (fun x -> if int_of_string x <> album_cache_version then raise Cache_version_mismatch)
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get albums id)
        | "name" -> new data (fun x -> name <- x)
        | "artist" -> new data (fun x -> artist <- x)
        | "artist-id" -> new data (fun x -> artist_id <- id_of_string x)
        | "album-type" -> new data (fun x -> album_type <- x)
        | "year" -> new data (fun x -> year <- int_of_string x)
        | "cover" -> new data (fun x -> cover <- Some (id_of_string x))
        | "copyright" -> new copyrights_parser (fun x -> copyrights <- x)
        | "restrictions" -> new nodes "restriction" parse_restriction (fun x -> restrictions <- x)
        | "external-ids" -> new nodes "external-id" parse_external_id (fun x -> external_ids <- x)
        | "discs" -> new nodes "disc" (fun attrs assign -> new disc_parser_from_cache assign) (fun x -> discs <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    match cached with
      | Some album ->
          assign album
      | None ->
          assign (Weak_cache.find albums id (fun () -> new album id (Album id) name artist artist_id album_type year cover copyrights restrictions external_ids discs))
end

let artists = Weak_cache.create 1024

class artist_parser session assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable name = ""
  val mutable portrait = None
  val mutable biographies = []
  val mutable similar_artists = []
  val mutable genres = []
  val mutable years_active = []
  val mutable albums = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get artists id)
        | "name" -> new data (fun x -> name <- x)
        | "portrait" -> new portrait_parser (fun x -> portrait <- Some x)
        | "bios" -> new nodes "bio" (fun attrs assign -> new biography_parser assign) (fun x -> biographies <- x)
        | "similar-artists" -> new nodes "similar-artist" (fun attrs assign -> new similar_artist_parser assign) (fun x -> similar_artists <- x)
        | "genres" -> new data (fun x -> genres <- split ',' x)
        | "years-active" -> new data (fun x -> years_active <- List.map int_of_string (split ',' x))
        | "albums" -> new nodes "album" (fun attrs assign -> new album_parser session (fun album -> assign album#id)) (fun x -> albums <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    let artist =
      match cached with
        | Some artist ->
            artist
        | None ->
            Weak_cache.find artists id (fun () -> new artist id (Artist id) name portrait biographies similar_artists genres years_active albums)
    in
    delay (save_artist session artist);
    assign artist
end

class artist_parser_from_cache assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable name = ""
  val mutable portrait = None
  val mutable biographies = []
  val mutable similar_artists = []
  val mutable genres = []
  val mutable years_active = []
  val mutable albums = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "mlspot-cache-version" -> new data (fun x -> if int_of_string x <> artist_cache_version then raise Cache_version_mismatch)
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get artists id)
        | "name" -> new data (fun x -> name <- x)
        | "portrait" -> new portrait_parser (fun x -> portrait <- Some x)
        | "bios" -> new nodes "bio" (fun attrs assign -> new biography_parser assign) (fun x -> biographies <- x)
        | "similar-artists" -> new nodes "similar-artist" (fun attrs assign -> new similar_artist_parser assign) (fun x -> similar_artists <- x)
        | "genres" -> new data (fun x -> genres <- split ',' x)
        | "years-active" -> new data (fun x -> years_active <- List.map int_of_string (split ',' x))
        | "albums" -> new nodes "album" (fun attrs assign -> new data (fun x -> assign (id_of_string x))) (fun x -> albums <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    match cached with
      | Some x ->
          assign x
      | None ->
          assign (Weak_cache.find artists id (fun () -> new artist id (Artist id) name portrait biographies similar_artists genres years_active albums))
end

let artist_searchs = Weak_cache.create 1024

class artist_search_parser assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable name = ""
  val mutable portrait = None
  val mutable popularity = 0.
  val mutable restrictions = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get artist_searchs id)
        | "name" -> new data (fun x -> name <- x)
        | "portrait" -> new portrait_parser (fun x -> portrait <- Some x)
        | "popularity" -> new data (fun x -> popularity <- float_of_string x)
        | "restrictions" -> new nodes "restriction" parse_restriction (fun x -> restrictions <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    match cached with
      | Some x ->
          assign x
      | None ->
          assign (Weak_cache.find artist_searchs id (fun () -> new artist_search id (Artist id) name portrait popularity restrictions))
end

let album_searchs = Weak_cache.create 1024

class album_search_parser assign = object
  inherit xml_parser

  val mutable id = dummy_id
  val mutable name = ""
  val mutable artist = ""
  val mutable artist_id = dummy_id
  val mutable cover = None
  val mutable popularity = 0.
  val mutable restrictions = []
  val mutable external_ids = []
  val mutable cached = None

  method node tag attrs =
    if cached = None then
      match tag with
        | "id" -> new data (fun x -> id <- id_of_string x; cached <- Weak_cache.get album_searchs id)
        | "name" -> new data (fun x -> name <- x)
        | "artist" -> new data (fun x -> artist <- x)
        | "artist-id" -> new data (fun x -> artist_id <- id_of_string x)
        | "cover" -> new data (fun x -> cover <- Some (id_of_string x))
        | "popularity" -> new data (fun x -> popularity <- float_of_string x)
        | "restrictions" -> new nodes "restriction" parse_restriction (fun x -> restrictions <- x)
        | "external-ids" -> new nodes "external-id" parse_external_id (fun x -> external_ids <- x)
        | _ -> new xml_parser
    else
      new xml_parser

  method stop =
    match cached with
      | Some x ->
          assign x
      | None ->
          assign (Weak_cache.find album_searchs id (fun () -> new album_search id (Album id) name artist artist_id cover popularity restrictions external_ids))
end

class search_result_callbacks_parser session query cb_artist cb_album cb_track assign = object
  inherit xml_parser

  val mutable did_you_mean = None
  val mutable total_artists = 0
  val mutable total_albums = 0
  val mutable total_tracks = 0

  method node tag attrs =
    match tag with
      | "did-you-mean" -> new data (fun x -> did_you_mean <- Some x)
      | "total-artists" -> new data (fun x -> total_artists <- int_of_string x)
      | "total-albums" -> new data (fun x -> total_albums <- int_of_string x)
      | "total-tracks" -> new data (fun x -> total_tracks <- int_of_string x)
      | "artists" -> new nodes "artist" (fun attrs assign -> new artist_search_parser cb_artist) ignore
      | "albums" -> new nodes "albums" (fun attrs assign -> new album_search_parser cb_album) ignore
      | "tracks" -> new nodes "tracks" (fun attrs assign -> new track_parser session cb_track) ignore
      | _ -> new xml_parser

  method stop =
    assign (new simple_search_result (Search query) did_you_mean total_artists total_albums total_tracks)
end

class search_result_parser session query assign = object
  inherit xml_parser

  val mutable did_you_mean = None
  val mutable total_artists = 0
  val mutable total_albums = 0
  val mutable total_tracks = 0
  val mutable artists = []
  val mutable albums = []
  val mutable tracks = []

  method node tag attrs =
    match tag with
      | "did-you-mean" -> new data (fun x -> did_you_mean <- Some x)
      | "total-artists" -> new data (fun x -> total_artists <- int_of_string x)
      | "total-albums" -> new data (fun x -> total_albums <- int_of_string x)
      | "total-tracks" -> new data (fun x -> total_tracks <- int_of_string x)
      | "artists" -> new nodes "artist" (fun attrs assign -> new artist_search_parser assign) (fun x -> artists <- x)
      | "albums" -> new nodes "album" (fun attrs assign -> new album_search_parser assign) (fun x -> albums <- x)
      | "tracks" -> new nodes "track" (fun attrs assign -> new track_parser session assign) (fun x -> tracks <- x)
      | _ -> new xml_parser

  method stop =
    assign (new search_result (Search query) did_you_mean total_artists total_albums total_tracks artists albums tracks)
end

class playlist_ops_parser assign = object
  inherit xml_parser

  val mutable name = ""
  val mutable destroyed = false
  val mutable tracks = []

  method node tag attrs =
    match tag with
      | "name" -> new data (fun x -> name <- x)
      | "add" -> new node "items" (fun attrs -> new data (fun x -> tracks <- List.map (fun str -> id_of_string (String.sub str 0 32))  (split ',' (strip x))))
      | "destroy" -> destroyed <- true; new xml_parser
      | _ -> new xml_parser

  method stop =
    assign (name, destroyed, tracks)
end

class playlist_change_parser assign = object
  inherit xml_parser

  val mutable name = ""
  val mutable user = ""
  val mutable time = 0.
  val mutable destroyed = false
  val mutable tracks = []

  method node tag attrs =
    match tag with
      | "ops" -> new playlist_ops_parser (fun (name', destroyed', tracks') ->
                                            name <- name';
                                            destroyed <- destroyed';
                                            tracks <- tracks')
      | "time" -> new data (fun x -> time <- float_of_string x)
      | "user" -> new data (fun x -> user <- x)
      | _ -> new xml_parser

  method stop =
    assign (name, user, time, destroyed, tracks)
end

class playlist_parser id assign = object
  inherit xml_parser

  val mutable name = ""
  val mutable user = ""
  val mutable time = 0.
  val mutable revision = 0
  val mutable checksum = 0
  val mutable collaborative = false
  val mutable destroyed = false
  val mutable tracks = []

  method node tag attrs =
    match tag with
      | "change" -> new playlist_change_parser (fun (name', user', time', destroyed', tracks') ->
                                                  name <- name';
                                                  user <- user';
                                                  time <- time';
                                                  destroyed <- destroyed;
                                                  tracks <- tracks')
      | "version" -> new data (fun x ->
                                 match split ',' x with
                                   | [revision'; num_tracks'; checksum'; collaborative'] ->
                                       revision <- int_of_string revision';
                                       checksum <- int_of_string checksum';
                                       collaborative <- int_of_string collaborative' <> 0
                                   | _ ->
                                       failwith "invalid playlist version")
      | _ -> new xml_parser

  method stop =
    assign (new playlist id (Playlist (user, id)) name user time revision checksum collaborative destroyed tracks)
end

class meta_playlist_parser assign = object
  inherit xml_parser

  val mutable user = ""
  val mutable time = 0.
  val mutable revision = 0
  val mutable checksum = 0
  val mutable collaborative = false
  val mutable playlists = []

  method node tag attrs =
    match tag with
      | "change" -> new playlist_change_parser (fun (name', user', time', destroyed', playlists') ->
                                                  user <- user';
                                                  time <- time';
                                                  playlists <- playlists')
      | "version" -> new data (fun x ->
                                 match split ',' x with
                                   | [revision'; num_tracks'; checksum'; collaborative'] ->
                                       revision <- int_of_string revision';
                                       checksum <- int_of_string checksum';
                                       collaborative <- int_of_string collaborative' <> 0
                                   | _ ->
                                       failwith "invalid playlist version")
      | _ -> new xml_parser

  method stop =
    assign (new meta_playlist user time revision checksum collaborative playlists)
end

(* +-----------------------------------------------------------------+
   | Dispatching                                                     |
   +-----------------------------------------------------------------+ *)

let dispatch sp command payload =
  if debug then delay (save_raw_data (string_of_command command) payload);
  match command with
    | CMD_SECRET_BLOCK ->
        send_packet sp CMD_CACHE_HASH "\xf4\xc2\xaa\x05\xe8\x25\xa7\xb5\xe4\xe6\x59\x0f\x3d\xd0\xbe\x0a\xef\x20\x51\x95"

    | CMD_PING ->
        send_packet sp CMD_PONG "\x00\x00\x00\x00"

    | CMD_PONG_ACK ->
        return ()

    | CMD_WELCOME ->
        wakeup sp.login_wakener ();
        return ()

    | CMD_PROD_INFO -> begin
        let cell = ref None in
        let products = XML.parse_string cell (new products_parser (fun x -> cell := Some x)) payload in
        try
          sp.set_products products;
          return ()
        with exn ->
          ignore (Lwt_log.error ~section ~exn "failed to set products informations");
          return ()
      end

    | CMD_COUNTRY_CODE -> begin
        try
          sp.set_country_code payload;
          return ()
        with exn ->
          ignore (Lwt_log.error ~section ~exn "failed to set the country code");
          return ()
      end

    | CMD_CHANNEL_DATA ->
        if String.length payload < 2 then
          Lwt_log.error ~section "invalid channel data received"
        else begin
          (* Read the channel id. *)
          let id = get_int16 payload 0 in
          match try Some (Channel_map.find id sp.channels) with Not_found -> None with
            | None ->
                Lwt_log.error ~section "channel data from unknown channel received"

            | Some channel ->
                if String.length payload = 2 then begin
                  (* End of channel. *)
                  release_channel sp id;
                  channel.ch_done <- true;
                  Lwt_condition.signal channel.ch_cond ();
                  return ()
                end else begin
                  Queue.add (payload, 2, String.length payload - 2) channel.ch_data;
                  Lwt_condition.signal channel.ch_cond ();
                  return ()
                end
        end

    | CMD_CHANNEL_ERROR ->
        if String.length payload < 2 then
          Lwt_log.error ~section "invalid channel error received"
        else begin
          (* Read the channel id. *)
          let id = get_int16 payload 0 in
          match try Some (Channel_map.find id sp.channels) with Not_found -> None with
            | None ->
                Lwt_log.error ~section "channel error from unknown channel received"

            | Some channel ->
                release_channel sp id;
                wakeup_exn channel.ch_error_wakener (Error "channel error");
                return ()
        end

    | CMD_AES_KEY ->
        if String.length payload < 4 then
          Lwt_log.error ~section "invalid AES key received"
        else begin
          let id = get_int16 payload 2 in
          match try Some (Channel_map.find id sp.channels) with Not_found -> None with
            | None ->
                Lwt_log.error ~section "AES key from unknown channel received"

            | Some channel ->
                release_channel sp id;
                Queue.add (payload, 4, String.length payload - 4) channel.ch_data;
                channel.ch_done <- true;
                Lwt_condition.signal channel.ch_cond ();
                return ()
        end

    | CMD_AES_KEY_ERROR ->
        if String.length payload < 4 then
          Lwt_log.error ~section "invalid AES key error received"
        else begin
          let id = get_int16 payload 2 in
          match try Some (Channel_map.find id sp.channels) with Not_found -> None with
            | None ->
                Lwt_log.error ~section "AES key error from unknown channel received"

            | Some channel ->
                release_channel sp id;
                wakeup_exn channel.ch_error_wakener (Error "key error");
                return ()
        end

    | CMD_PLAYLIST_CHANGED ->
        if String.length payload <> 17 then
          Lwt_log.error ~section "invalid playlist notification received"
        else if payload = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" then
          match sp.meta_playlist with
            | Some (notify, weak) ->
                notify ();
                return ()
            | None ->
                return ()
        else begin
          let id = ID.of_bytes (String.sub payload 0 16) in
          match try Some (ID_table.find sp.playlists id) with Not_found -> None with
            | Some (notify, weak) ->
                notify ();
                return ()
            | None ->
                return ()
        end

    | _ ->
        Lwt_log.info_f ~section "do not know what to do with command '%s'" (string_of_command command)

let disconnect sp =
  if not sp.disconnected then begin
    sp.disconnected <- true;
    wakeup_exn sp.logout_wakener Disconnected;
    try_lwt
      Lwt_io.flush sp.oc
    finally
      Lwt_unix.shutdown sp.socket Unix.SHUTDOWN_ALL;
      Lwt_unix.close sp.socket
  end else
    return ()

let rec loop_dispatch sp =
  lwt continue =
    try_lwt
      lwt command, payload = recv_packet sp in
      ignore (
        try_lwt
          dispatch sp command payload
        with exn ->
          Lwt_log.error ~section ~exn "dispatcher failed with"
      );
      return true
    with
      | Unknown_command command ->
          ignore (Lwt_log.error_f ~section "unknown command received (0x%02x)" command);
          return true
      | Logged_out | Disconnected ->
          return false
      | End_of_file ->
          lwt () = disconnect sp in
          return false
      | exn ->
          ignore (Lwt_log.error ~section ~exn "command reader failed with");
          lwt () = disconnect sp in
          return false
  in
  if continue then
    loop_dispatch sp
  else
    return ()

(* +-----------------------------------------------------------------+
   | Loggin/logout                                                   |
   +-----------------------------------------------------------------+ *)

exception Connection_failure of string

let dh_parameters = {
  Cryptokit.DH.p = "\
\xff\xff\xff\xff\xff\xff\xff\xff\xc9\x0f\xda\xa2\x21\x68\xc2\x34\
\xc4\xc6\x62\x8b\x80\xdc\x1c\xd1\x29\x02\x4e\x08\x8a\x67\xcc\x74\
\x02\x0b\xbe\xa6\x3b\x13\x9b\x22\x51\x4a\x08\x79\x8e\x34\x04\xdd\
\xef\x95\x19\xb3\xcd\x3a\x43\x1b\x30\x2b\x0a\x6d\xf2\x5f\x14\x37\
\x4f\xe1\x35\x6d\x6d\x51\xc2\x45\xe4\x85\xb5\x76\x62\x5e\x7e\xc6\
\xf4\x4c\x42\xe9\xa6\x3a\x36\x20\xff\xff\xff\xff\xff\xff\xff\xff";
  Cryptokit.DH.g = "\x02";
  Cryptokit.DH.privlen = 160;
}

let key = "\
\xae\xd6\xb1\xf6\xf2\x67\x44\xdb\xf1\x79\x85\x30\xc9\xfc\xcc\x35\
\xb0\x86\x81\x0d\x1d\x87\x2f\x0f\xba\xa8\xbe\x71\x7a\x43\xac\xc6\
\xd3\x73\x88\x2c\xdf\x3b\x6e\x96\x56\xae\x8b\x18\x93\x56\x77\xb1\
\xa0\xe0\x4e\xea\xa7\xea\x1d\xbd\xb1\xc5\x95\xb4\x30\xc3\x31\xf1\
\x57\x4f\x0e\xb8\xc8\x3a\xd6\xb3\x9a\xab\x71\xdc\xbb\x3b\x4d\xb9\
\x84\x31\x65\x18\x79\x13\x73\xe8\xa6\xe3\xc8\x9b\x56\xcf\x25\x8e\
\x49\x83\x2d\xe1\x84\xcd\xd4\x5d\x06\xe4\x41\x99\xc7\x0b\x37\x5c\
\x08\x3e\xdc\x86\x8d\xb0\x21\xbe\xc1\x53\xcb\xf8\xcc\x4f\x40\x13"

let logout session =
  match session#get_session_parameters with
    | None ->
        return ()
    | Some sp ->
        session#set_session_parameters None;
        disconnect sp

let login session ~username ~password =
  lwt () = logout session in

  lwt socket =
    try_lwt
      (* Service lookup. *)
      lwt servers =
        try_lwt
          service_lookup ()
        with Unix.Unix_error (error, _, _) ->
          ignore (Lwt_log.warning_f ~section "service lookup failed: %s" (Unix.error_message error));
          return [(0, "ap.spotify.com", 4070)]
      in

      (* Sort servers. *)
      let servers = List.sort (fun (prio1, _, _) (prio2, _, _) -> prio1 - prio2) servers in

      (* Read address informations. *)
      lwt address_infos =
        Lwt_list.map_p
          (fun (priority, host, port) ->
             Lwt_unix.getaddrinfo host (string_of_int port) [Unix.AI_SOCKTYPE Unix.SOCK_STREAM; Unix.AI_PROTOCOL 0])
          servers
        >|= List.flatten
      in

      (* Try to connect to a server. *)
      let rec loop address_infos =
        match address_infos with
          | [] ->
              raise_lwt (Connection_failure "no server available")
          | address_info :: address_infos ->
              let sock = Lwt_unix.socket address_info.Unix.ai_family address_info.Unix.ai_socktype address_info.Unix.ai_protocol in
              try_lwt
                (* Try to connect. *)
                lwt () = Lwt_unix.connect sock address_info.Unix.ai_addr in
                return sock
              with Unix.Unix_error _ ->
                lwt () = Lwt_unix.close sock in
                (* Try other addresses. *)
                loop address_infos
      in

      loop address_infos

    with Unix.Unix_error (error, _, _) ->
      raise_lwt (Connection_failure (Unix.error_message error))
  in

  try_lwt
    let ic = Lwt_io.make ~mode:Lwt_io.input (Lwt_bytes.read socket)
    and oc = Lwt_io.make ~mode:Lwt_io.output (Lwt_bytes.write socket) in

    (* Create a new random number generator.
       [Cryptokit.Random.secure_rng] is far two slow, so we get some
       random random bytes from /dev/random and complete with pseudo
       random data. *)
    let random = String.create 55 in
    let offset =
      try
        let fd = Unix.openfile "/dev/random" [Unix.O_RDONLY; Unix.O_NONBLOCK] 0 in
        begin
          try
            let offset = Unix.read fd random 0 (String.length random) in
            Unix.close fd;
            ignore (Lwt_log.info_f ~section "%d random bytes read from /dev/random" offset);
            offset
          with _ ->
            ignore (Lwt_log.info_f ~section "0 random bytes read from /dev/random");
            Unix.close fd;
            0
        end
      with _ ->
        0
    in
    if offset < String.length random then begin
      let state = Random.State.make_self_init () in
      for i = offset to String.length random - 1 do
        random.[i] <- Char.unsafe_chr (Random.State.int state 256)
      done;
    end;

    (* Create the random number generator. *)
    let rng = Cryptokit.Random.pseudo_rng random in

    (* Generate random data. *)
    let client_random = Cryptokit.Random.string rng 16 in

    (* Generate a secret. *)
    let secret = Cryptokit.DH.private_secret ~rng dh_parameters in

    (* Forge the initial packet. *)
    let packet = Packet.create () in

    (* Protocol version. *)
    Packet.add_int16 packet 3;
    (* Packet length, updated later. *)
    Packet.add_int16 packet 0;
    (* Unknown. *)
    Packet.add_int32 packet 0x00000300;
    (* Unknown. *)
    Packet.add_int32 packet 0x00030c00;
    (* Client revision. *)
    Packet.add_int32 packet 99999;
    (* Unknown. *)
    Packet.add_int32 packet 0;
    (* Unknown. *)
    Packet.add_int32 packet 0x01000000;
    (* Client ID. *)
    Packet.add_int32 packet 0x01040101;
    (* Unknown. *)
    Packet.add_int32 packet 0;
    (* Random data. *)
    Packet.add_string packet client_random;
    (* DH message. *)
    Packet.add_string packet (Cryptokit.DH.message dh_parameters secret);
    (* RSA modulus. *)
    Packet.add_string packet key;
    (* Length of random data. *)
    Packet.add_int8 packet 0;
    Packet.add_int8 packet (String.length username);
    (* Unknown. *)
    Packet.add_int16 packet 0x0100;
    (* <-- random data would go here *)
    Packet.add_string packet username;
    (* Unknown. *)
    Packet.add_int8 packet 0x40;
    (* Update length bytes. *)
    Packet.put_int16 packet 2 (Packet.length packet);

    (* Save the initial client packet. *)
    let init_client = Packet.contents packet in

    (* Send the packet. *)
    lwt () = Packet.send oc packet in
    lwt () = Lwt_io.flush oc in

    Packet.reset packet;

    let server_random = String.create 16 in

    (* Read 2 status bytes. *)
    lwt () = Lwt_io.read_into_exactly ic server_random 0 2 in

    if server_random.[0] <> '\x00' then begin
      match Char.code server_random.[1] with
        | 0x01 -> raise (Authentication_failure "client upgrade recquired")
        | 0x03 -> raise (Authentication_failure "user not found")
        | 0x04 -> raise (Authentication_failure "account has been disabled")
        | 0x06 -> raise (Authentication_failure "you need to complete your account details")
        | 0x09 -> raise (Authentication_failure "country mismatch")
        | code -> raise (Authentication_failure (Printf.sprintf "unknown error (%d)" code))
    end;

    (* Read remaining 14 random bytes. *)
    lwt () = Lwt_io.read_into_exactly ic server_random 2 14 in
    Packet.add_string packet server_random;

    let public_key = String.create 96 in
    lwt () = Lwt_io.read_into_exactly ic public_key 0 96 in
    Packet.add_string packet public_key;

    let blob = String.create 256 in
    lwt () = Lwt_io.read_into_exactly ic blob 0 256 in
    Packet.add_string packet blob;

    let salt = String.create 10 in
    lwt () = Lwt_io.read_into_exactly ic salt 0 10 in
    Packet.add_string packet salt;

    lwt padding_length = Lwt_io.read_char ic >|= Char.code in
    Packet.add_int8 packet padding_length;
    lwt username_length = Lwt_io.read_char ic >|= Char.code in
    Packet.add_int8 packet username_length;
    let puzzle_length = ref 0 in
    lwt () =
      for_lwt i = 0 to 3 do
        lwt len0 = Lwt_io.read_char ic >|= Char.code in
        lwt len1 = Lwt_io.read_char ic >|= Char.code in
        Packet.add_int8 packet len0;
        Packet.add_int8 packet len1;
        puzzle_length := !puzzle_length + ((len0 lsl 8) lor len1);
        return ()
      done
    in

    let padding = String.create padding_length in
    lwt () = Lwt_io.read_into_exactly ic padding 0 padding_length in
    Packet.add_string packet padding;

    let username = String.create username_length in
    lwt () = Lwt_io.read_into_exactly ic username 0 username_length in
    Packet.add_string packet username;

    let puzzle = String.create !puzzle_length in
    lwt () = Lwt_io.read_into_exactly ic puzzle 0 !puzzle_length in
    Packet.add_string packet puzzle;

    (* Save the initial server packet. *)
    let init_server = Packet.contents packet in

    if String.length puzzle < 6 || puzzle.[0] <> '\x01' then
      raise (Authentication_failure "unexpected puzzle challenge");
    let denominator = 1 lsl (Char.code puzzle.[1]) - 1 in
    let magic = get_int32 puzzle 2 in

    (* Compute the shared secret. *)
    let shared_secret = Cryptokit.DH.shared_secret dh_parameters secret public_key in

    (* Hash of the salt and the password. *)
    let ctx = Cryptokit.Hash.sha1 () in
    ctx#add_string salt;
    ctx#add_char ' ';
    ctx#add_string password;
    let auth_hash = ctx#result in

    let message = auth_hash ^ client_random ^ server_random ^ "\x00" in

    let hmac_output = String.create (20 * 5) in
    for i = 1 to 5 do
      message.[52] <- char_of_int i;
      let ctx = Cryptokit.MAC.hmac_sha1 shared_secret in
      ctx#add_string message;
      let hmac = ctx#result in
      String.blit hmac 0 hmac_output ((i - 1) * 20) 20;
      String.blit hmac 0 message 0 20
    done;

    let shn_send = shn_ctx_new () in
    let shn_recv = shn_ctx_new () in
    shn_key shn_send hmac_output 20 32;
    shn_key shn_recv hmac_output 52 32;

    let key_hmac = String.sub hmac_output 0 20 in

    (* Solve the puzzle. *)
    let solution = String.create 8 in
    Random.self_init ();
    let rec loop () =
      let ctx = Cryptokit.Hash.sha1 () in
      ctx#add_string server_random;
      for i = 0 to 7 do
        solution.[i] <- char_of_int (Random.int 256)
      done;
      ctx#add_string solution;
      let digest = ctx#result in
      let nominator = get_int32 digest 16 in
      if (nominator lxor magic) land denominator <> 0 then loop ()
    in
    loop ();

    Packet.reset packet;
    Packet.add_string packet init_client;
    Packet.add_string packet init_server;
    (* Random data length. *)
    Packet.add_int8 packet 0;
    (* Unknown. *)
    Packet.add_int8 packet 0;
    (* Puzzle solution length. *)
    Packet.add_int16 packet 8;
    (* Unknown. *)
    Packet.add_int32 packet 0;
    (* <-- random data would go here. *)
    Packet.add_string packet solution;

    let ctx = Cryptokit.MAC.hmac_sha1 key_hmac in
    ctx#add_substring (Packet.buffer packet) 0 (Packet.length packet);
    let auth_hmac = ctx#result in

    (* Forge the authentication packet. *)
    Packet.reset packet;

    Packet.add_string packet auth_hmac;
    (* Random data length. *)
    Packet.add_int8 packet 0;
    (* Unknown. *)
    Packet.add_int8 packet 0;
    (* Puzzle solution length. *)
    Packet.add_int16 packet 8;
    (* Unknown. *)
    Packet.add_int32 packet 0;
    (* <-- random data would go here. *)
    Packet.add_string packet solution;

    (* Send the packet. *)
    lwt () = Packet.send oc packet in

    (* Read the response. *)
    lwt status = Lwt_io.read_char ic >|= Char.code in
    if status <> 0 then raise (Authentication_failure "authentication failed");

    (* Read the payload length. *)
    lwt payload_length = Lwt_io.read_char ic >|= Char.code in

    (* Read the payload. *)
    let payload = String.create payload_length in
    lwt () = Lwt_io.read_into_exactly ic payload 0 payload_length in

    let login_waiter, login_wakener = task () in
    let logout_waiter, logout_wakener = wait () in
    let sp = {
      disconnected = false;
      socket;
      ic;
      oc;
      shn_send;
      shn_recv;
      send_iv = 0;
      recv_iv = 0;
      channels = Channel_map.empty;
      next_channel_id = 0;
      channel_released = Lwt_condition.create ();
      login_waiter;
      login_wakener;
      logout_waiter;
      logout_wakener;
      set_products = session#set_products;
      set_country_code = session#set_country_code;
      playlists = ID_table.create 32;
      meta_playlist = None;
    } in

    (* Start the dispatcher. *)
    ignore (loop_dispatch sp);

    try_lwt
      lwt () = login_waiter in
      session#set_session_parameters (Some sp);
      return ()

    with exn ->
      (* Make sure the dispatcher exit. *)
      wakeup_exn logout_wakener Logged_out;
      raise_lwt exn

  with exn ->
    Lwt_unix.shutdown socket Unix.SHUTDOWN_ALL;
    lwt () = Lwt_unix.close socket in
    raise_lwt exn

(* +-----------------------------------------------------------------+
   | Commands                                                        |
   +-----------------------------------------------------------------+ *)

let save_debug_stream kind stream =
  if debug then
    lwt string = string_of_stream stream in
    delay (save_raw_data kind string);
    return (new stream_of_string string 0 (String.length string))
  else
    return stream

let rec browse : 'a. session -> id list -> string -> int -> (('a -> unit) -> xml_parser) -> (('a list -> unit) -> xml_parser) -> 'a list Lwt.t = fun session ids kind kind_id cache_parser fresh_parser ->
  if List.exists (fun id -> id_length id <> 16) ids then raise (Wrong_id kind);
  (* Try to fetch as much elements as possible from the cache. *)
  lwt cached, ids =
    Lwt_list.map_p
      (fun id ->
         let filename = make_filename ["metadata"; kind ^ "s"; string_of_id id ^ ".xml.gz"] in
         match_lwt Cache.get_reader session filename with
           | None ->
               return (Inr id)
           | Some (file, fd) ->
               try_lwt
                 let cell = ref None in
                 lwt x = XML.parse_stream cell (cache_parser (fun x -> cell := Some x)) (new inflate_stream (new stream_of_file file fd)) in
                 return (Inl x)
               with
                 | Cache_version_mismatch ->
                     ignore (Lwt_log.warning_f ~section "the cache file %S does not match the version of mlspot, removing it" filename);
                     lwt () = safe_unlink file in
                     return (Inr id)
                 | exn ->
                     ignore (Lwt_log.warning_f ~section ~exn "failed to read %S from the cache, removing it" filename);
                     lwt () = safe_unlink file in
                     return (Inr id)
               finally
                 Cache.release ();
                 return ())
      ids
    >|= split_either
  in
  match ids with
    | [] ->
        return cached
    | _ ->
        (* Fetch the rest from spotify. *)
        lwt channel_id, stream = alloc_channel session#session_parameters in
        let packet = Packet.create () in
        Packet.add_int16 packet channel_id;
        Packet.add_int8 packet kind_id;
        List.iter (fun id -> Packet.add_string packet (ID.to_bytes id)) ids;
        if kind_id = 1 || kind_id = 2 then Packet.add_int32 packet 0;
        lwt () = send_packet session#session_parameters CMD_BROWSE (Packet.contents packet) in
        lwt stream = save_debug_stream kind stream in
        let cell = ref None in
        try_lwt
          lwt x = XML.parse_stream cell (fresh_parser (fun x -> cell := Some x)) (new inflate_stream stream) in
          return (cached @ x)
        with exn ->
          ignore (Lwt_log.warning_f ~section ~exn "failex to parse %s XML" kind);
          raise_lwt exn

and get_artist session id =
  Weak_cache.find_lwt artists id
    (fun () ->
       match_lwt
         browse session [id] "artist" 1
           (fun assign -> new node "artist" (fun attrs -> new artist_parser_from_cache assign))
           (fun assign -> new node "artist" (fun attrs -> new artist_parser session (fun x -> assign [x])))
       with
         | [artist] ->
             return artist
         | _ ->
             assert false)

and get_album session id =
  Weak_cache.find_lwt albums id
    (fun () ->
       match_lwt
         browse session [id] "album" 2
           (fun assign -> new node "album" (fun attrs -> new album_parser_from_cache assign))
           (fun assign -> new node "album" (fun attrs -> new album_parser session (fun x -> assign [x])))
       with
         | [album] ->
             return album
         | _ ->
             assert false)

and get_tracks session ids =
  Weak_cache.find_multiple_lwt tracks ids
    (fun ids ->
       browse session ids "track" 3
         (fun assign -> new node "track" (fun attrs -> new track_parser session assign))
         (fun assign -> new node "result" (fun attrs -> new nodes "tracks" (fun attrs assign -> new node "track" (fun attrs -> new track_parser session assign)) assign)))

let get_track session id =
  lwt tracks = get_tracks session [id] in
  return (List.hd tracks)

let images = Weak_cache.create 1024

let get_image session id =
  if id_length id <> 20 then raise (Wrong_id "image");
  Weak_cache.find_lwt images id
    (fun () ->
       let filename = Filename.concat "images" (string_of_id id ^ ".jpg") in
       match_lwt Cache.load session filename with
         | Some data ->
             return data
         | None ->
             lwt channel_id, stream = alloc_channel session#session_parameters in
             try_lwt
               let packet = Packet.create () in
               Packet.add_int16 packet channel_id;
               Packet.add_string packet (ID.to_bytes id);
               lwt () = send_packet session#session_parameters CMD_IMAGE (Packet.contents packet) in
               lwt data = string_of_stream stream in
               delay (Cache.save session filename data);
               return data)

let search session ?(offset = 0) ?(length = 1000) query =
  let len = String.length query in
  if len > 255 then invalid_arg "Spotify.search";
  lwt channel_id, stream = alloc_channel session#session_parameters in
  let packet = Packet.create () in
  Packet.add_int16 packet channel_id;
  Packet.add_int32 packet offset;
  Packet.add_int32 packet length;
  Packet.add_int16 packet 0;
  Packet.add_int8 packet len;
  Packet.add_string packet query;
  lwt () = send_packet session#session_parameters CMD_SEARCH (Packet.contents packet) in
  lwt stream = save_debug_stream "search result" stream in
  let cell = ref None in
  XML.parse_stream cell (new node "result" (fun attrs -> new search_result_parser session query (fun x -> cell := Some x))) (new inflate_stream stream)

let search_callbacks session ?(offset = 0) ?(length = 1000) ?(artist = ignore) ?(album = ignore) ?(track = ignore) query =
  let len = String.length query in
  if len > 255 then invalid_arg "Spotify.search";
  lwt channel_id, stream = alloc_channel session#session_parameters in
  let packet = Packet.create () in
  Packet.add_int16 packet channel_id;
  Packet.add_int32 packet offset;
  Packet.add_int32 packet length;
  Packet.add_int16 packet 0;
  Packet.add_int8 packet len;
  Packet.add_string packet query;
  lwt () = send_packet session#session_parameters CMD_SEARCH (Packet.contents packet) in
  lwt stream = save_debug_stream "search result" stream in
  let cell = ref None in
  XML.parse_stream cell (new node "result" (fun attrs -> new search_result_callbacks_parser session query artist album track (fun x -> cell := Some x))) (new inflate_stream stream)

(* +-----------------------------------------------------------------+
   | Playlists                                                       |
   +-----------------------------------------------------------------+ *)

let remove_playlist session id () =
  match session#get_session_parameters with
    | Some sp ->
        ID_table.remove sp.playlists id
    | None ->
        ()

let fetch_playlist session id =
  lwt channel_id, stream = alloc_channel session#session_parameters in
  let packet = Packet.create () in
  Packet.add_int16 packet channel_id;
  Packet.add_string packet (ID.to_bytes id);
  Packet.add_int8 packet 2;
  Packet.add_int32 packet (-1);
  Packet.add_int32 packet 0;
  Packet.add_int32 packet (-1);
  Packet.add_int8 packet 1;
  lwt () = send_packet session#session_parameters CMD_GET_DATA_PLAYLIST (Packet.contents packet) in
  lwt stream = save_debug_stream "playlist" stream in
  let cell = ref None in
  XML.parse_stream ~buggy_root:true cell (new node "next-change" (fun attrs -> new playlist_parser id (fun x -> cell := Some x))) stream

let get_playlist session id =
  if id_length id <> 16 then raise (Wrong_id "playlist");
  match try Weak.get (snd (ID_table.find session#session_parameters.playlists id)) 0 with Not_found -> None with
    | Some signal ->
        return signal
    | None ->
        lwt playlist = fetch_playlist session id in
        let sp = session#session_parameters in
        match try Weak.get (snd (ID_table.find sp.playlists id)) 0 with Not_found -> None with
          | Some signal ->
              return signal
          | None ->
              let ev, notify = E.create () in
              let signal = S.with_finaliser (remove_playlist session id) (S.hold playlist (E.map_s (fun () -> fetch_playlist session id) ev)) in
              let weak = Weak.create 1 in
              Weak.set weak 0 (Some signal);
              ID_table.add sp.playlists id (notify, weak);
              return signal

let remove_meta_playlist session () =
  match session#get_session_parameters with
    | Some sp ->
        sp.meta_playlist <- None
    | None ->
        ()

let fetch_meta_playlist session =
  lwt channel_id, stream = alloc_channel session#session_parameters in
  let packet = Packet.create () in
  Packet.add_int16 packet channel_id;
  for i = 1 to 17 do
    Packet.add_int8 packet 0
  done;
  Packet.add_int32 packet (-1);
  Packet.add_int32 packet 0;
  Packet.add_int32 packet (-1);
  Packet.add_int8 packet 1;
  lwt () = send_packet session#session_parameters CMD_GET_DATA_PLAYLIST (Packet.contents packet) in
  lwt stream = save_debug_stream "meta playlist" stream in
  let cell = ref None in
  XML.parse_stream ~buggy_root:true cell (new node "next-change" (fun attrs -> new meta_playlist_parser (fun x -> cell := Some x))) stream

let get_meta_playlist session =
  match
    match session#session_parameters.meta_playlist with
      | Some (notify, weak) ->
          Weak.get weak 0
      | None ->
          None
  with
    | Some signal ->
        return signal
    | None ->
        lwt playlist = fetch_meta_playlist session in
        let sp = session#session_parameters in
        match
          match sp.meta_playlist with
            | Some (notify, weak) ->
                Weak.get weak 0
            | None ->
                None
        with
          | Some signal ->
              return signal
          | None ->
              let ev, notify = E.create () in
              let signal = S.with_finaliser (remove_meta_playlist session) (S.hold playlist (E.map_s (fun () -> fetch_meta_playlist session) ev)) in
              let weak = Weak.create 1 in
              Weak.set weak 0 (Some signal);
              sp.meta_playlist <- Some (notify, weak);
              return signal

(* +-----------------------------------------------------------------+
   | Data fetching                                                   |
   +-----------------------------------------------------------------+ *)

let request_key session track_id file_id =
  lwt channel_id, stream = alloc_channel ~header:false session#session_parameters in
  let packet = Packet.create () in
  Packet.add_string packet (ID.to_bytes file_id);
  Packet.add_string packet (ID.to_bytes track_id);
  Packet.add_int16 packet 0;
  Packet.add_int16 packet channel_id;
  lwt () = send_packet session#session_parameters CMD_REQUEST_KEY (Packet.contents packet) in
  lwt key = string_of_stream stream in
  return (rijndael_key_setup_enc key)

let get_char str ofs =
  if ofs >= 0 && ofs < String.length str then
    String.unsafe_get str ofs
  else
    '\x00'

let set_char str ofs chr =
  if ofs >= 0 && ofs < String.length str then
    String.unsafe_set str ofs chr

(* Fetch one part of a file from spotify. *)
let fetch session file_id offset length aes iv =
  lwt channel_id, stream = alloc_channel session#session_parameters in
  let packet = Packet.create () in
  Packet.add_int16 packet channel_id;
  Packet.add_int16 packet 0x0800;
  Packet.add_int16 packet 0x0000;
  Packet.add_int16 packet 0x0000;
  Packet.add_int16 packet 0x0000;
  Packet.add_int16 packet 0x4e20;
  Packet.add_int32 packet (200 * 1000);
  Packet.add_string packet (ID.to_bytes file_id);
  Packet.add_int32 packet (offset / 4);
  Packet.add_int32 packet ((offset + length) / 4);
  lwt () = send_packet session#session_parameters CMD_GET_DATA (Packet.contents packet) in
  lwt data = string_of_stream stream in
  let len = String.length data in
  let plain = String.create len in
  let keystream = String.create 16 in
  (* Decrypt each 1024 block. *)
  for block = 0 to (len - 1) / 1024 do
    let block_size = min 1024 (len - block * 1024) in

    (* Deinterleave the 4x256 byte blocks *)
    for i = 0 to (block_size - 1) / 4 do
      set_char plain (block * 1024 + i * 4 + 0) (get_char data (block * 1024 + 0 * 256 + i));
      set_char plain (block * 1024 + i * 4 + 1) (get_char data (block * 1024 + 1 * 256 + i));
      set_char plain (block * 1024 + i * 4 + 2) (get_char data (block * 1024 + 2 * 256 + i));
      set_char plain (block * 1024 + i * 4 + 3) (get_char data (block * 1024 + 3 * 256 + i))
    done;

    (* Decrypt 1024 bytes block. *)
    for i = 0 to (block_size - 1) / 16 do
      (* Produce 16 bytes of keystream from the IV. *)
      rijndael_encrypt aes iv keystream;

      (* Update IV counter. *)
      let rec loop i =
        if i >= 0 then begin
          let x = (Char.code iv.[i] + 1) land 0xff in
          iv.[i] <- Char.unsafe_chr x;
          if x = 0 then loop (i - 1)
        end
      in
      loop 15;

      (* Produce plaintext by XORing ciphertext with keystream. *)
      for j = 0 to 15 do
        set_char plain (block * 1024 + i * 16 + j) (Char.unsafe_chr ((Char.code (get_char plain (block * 1024 + i * 16 + j)) lxor (Char.code keystream.[j]))))
      done
    done
  done;
  return plain

(* +-----------------------------------------------------------------+
   | Streaming                                                       |
   +-----------------------------------------------------------------+ *)

exception Stream_closed

(* Ogg page streamer. *)
class virtual page_stream = object(self)
  method virtual read : string -> int -> int -> unit Lwt.t
    (* [read_bytes buf ofs len] reads exactly [len] bytes and store
       them into [buf] at position [ofs]. *)

  method virtual close : unit Lwt.t
    (* Closes the source of the stream. *)

  val mutable first = true
    (* Has the first page been read ? *)

  val page_buffer = String.create (27 + 255)
    (* Buffer used to read one header. *)

  (* Read one ogg page. *)
  method get_page =
    (* Read the part of the header of constant length. *)
    lwt () = self#read page_buffer 0 27 in
    (* Extract the number of segments. *)
    let num_segments = Char.code page_buffer.[26] in
    (* Create the buffer for the header. *)
    let header = String.create (27 + num_segments) in
    (* Copy the first part of the header. *)
    String.unsafe_blit page_buffer 0 header 0 27;
    (* Read segments. *)
    lwt () = self#read header 27 num_segments in
    (* Compute the length of the body. *)
    let body_length = ref 0 in
    for i = 0 to num_segments - 1 do
      body_length := !body_length + Char.code header.[27 + i]
    done;
    (* Read the body of the page. *)
    let body = String.create !body_length in
    lwt () = self#read body 0 !body_length in
    (* Skip the first page if needed. *)
    if first then begin
      first <- false;
      if header.[5] = '\x06' then
        self#get_page
      else
        return (header, body)
    end else
      return (header, body)
end

type stream_parameters = {
  page_stream : page_stream;
  ogg_stream : Ogg.Stream.t;
  decoder : Vorbis.Decoder.t;
  close_waiter : unit Lwt.t;
  close_wakener : unit Lwt.u;
  vorbis_info : Vorbis.info;
}

class stream parameters = object
  val mutable parameters_opt = Some parameters
  method parameters =
    match parameters_opt with
      | Some parameters -> parameters
      | None -> raise Stream_closed
  method parameters_opt = parameters
  method close =
    match parameters_opt with
      | Some sp ->
          parameters_opt <- None;
          wakeup_exn sp.close_wakener Stream_closed;
          sp.page_stream#close
      | None ->
          return ()
end

(* Read one packet from the given ogg stream. *)
let rec get_packet ogg_stream page_stream =
  try
    return (Ogg.Stream.get_packet ogg_stream)
  with Ogg.Not_enough_data ->
    lwt page = page_stream#get_page in
    Ogg.Stream.put_page ogg_stream page;
    get_packet ogg_stream page_stream

let create_stream page_stream =
  try_lwt
    lwt page = page_stream#get_page in
    (* Read the serial of the stream. *)
    let serial = Ogg.Page.serialno page in
    (* Create the ogg stream. *)
    let ogg_stream = Ogg.Stream.create ~serial () in
    (* Feed it with the first page. *)
    Ogg.Stream.put_page ogg_stream page;
    (* Read the three first packets. *)
    lwt packet1 = get_packet ogg_stream page_stream in
    lwt packet2 = get_packet ogg_stream page_stream in
    lwt packet3 = get_packet ogg_stream page_stream in
    let decoder = Vorbis.Decoder.init packet1 packet2 packet3 in
    let info = Vorbis.Decoder.info decoder in
    let close_waiter, close_wakener = wait () in
    return (new stream {
              page_stream;
              ogg_stream;
              decoder;
              close_waiter;
              close_wakener;
              vorbis_info = info;
            })
  with exn ->
    ignore (Lwt_log.error ~section ~exn "failed to create ogg vorbis stream");
    raise_lwt (Error "failed to create vorbis stream")

let rec read stream buffer offset length =
  let sp = stream#parameters in
  try
    return (Vorbis.Decoder.decode_pcm sp.decoder sp.ogg_stream buffer offset length)
  with Ogg.Not_enough_data ->
    lwt page = pick [sp.page_stream#get_page; sp.close_waiter >> fail Exit] in
    Ogg.Stream.put_page sp.ogg_stream page;
    read stream buffer offset length

let seek stream = assert false
let position stream = assert false

let channels stream = stream#parameters.vorbis_info.Vorbis.audio_channels
let sample_rate stream = stream#parameters.vorbis_info.Vorbis.audio_samplerate
let vorbis_info stream = stream#parameters.vorbis_info

let close stream = stream#close

(* +-----------------------------------------------------------------+
   | Streaming from the disk                                         |
   +-----------------------------------------------------------------+ *)

class page_stream_from_fd fd = object
  inherit page_stream

  val ic = Lwt_io.of_fd ~mode:Lwt_io.input fd

  method read buf ofs len =
    Lwt_io.read_into_exactly ic buf ofs len

  method close =
    Lwt_io.close ic
end

(* +-----------------------------------------------------------------+
   | Streaming from spotify                                          |
   +-----------------------------------------------------------------+ *)

let block_size = 16384
  (* Size of blocks fetched from spotify. *)

class page_stream_from_spotify session file_id aes = object(self)
  inherit page_stream

  val iv = "\x72\xe0\x67\xfb\xdd\xcb\xcf\x77\xeb\xe8\xbc\x64\x3f\x63\x0d\x93"

  val mutable global_offset = 0
    (* Offset in the file. *)

  val mutable buffer = ""
    (* Data fetched from spotify. *)
  val mutable offset = 0
    (* Offset of the beginning of data not yet consumed in
       [buffer]. *)
  val mutable length = 0
    (* Length of [buffer]. *)
  val mutable end_of_stream = false
    (* Whether the end of stream has been reached. *)

  method fetch offset =
    fetch session file_id offset block_size aes iv

  method read buf ofs len =
    let avail = length - offset in
    if avail >= len then begin
      String.unsafe_blit buffer offset buf ofs len;
      offset <- offset + len;
      return ()
    end else if end_of_stream then
      raise_lwt End_of_file
    else begin
      String.unsafe_blit buffer offset buf ofs avail;
      lwt data = self#fetch global_offset in
      if String.length data < block_size then end_of_stream <- true;
      offset <- 0;
      buffer <- data;
      length <- String.length buffer;
      global_offset <- global_offset + length;
      self#read buf (ofs + avail) (len - avail)
    end

  method close =
    buffer <- "";
    offset <- 0;
    length <- 0;
    return ()
end

module Int_set = Set.Make (struct type t = int let compare a b = a - b end)
module Int_map = Map.Make (struct type t = int let compare a b = a - b end)

let write_string fd str =
  let rec loop ofs len =
    lwt n = Lwt_unix.write fd str ofs len in
    if n < len then
      loop (ofs + n) (len - n)
    else
      return ()
  in
  loop 0 (String.length str)

let read_string fd len =
  let str = String.create len in
  let rec loop ofs len =
    lwt n = Lwt_unix.read fd str ofs len in
    if n = 0 then
      return (String.sub str 0 ofs)
    else if n < len then
      loop (ofs + n) (len - n)
    else
      return str
  in
  loop 0 len

class page_stream_from_spotify_with_cache session file_id aes file rfd wfd = object(self)
  inherit page_stream_from_spotify session file_id aes as super

  val mutable cached = Int_set.empty
    (* Set of blocks cached. *)

  val mutable skipped = Int_map.empty
    (* Map from offset of blocks skipped by the cache filler to their
       length. *)

  val mutable r_offset = 0
    (* Offset in [rfd]. *)

  val mutable w_offset = 0
    (* Offset in [wfd]. *)

  val mutable next_wanted = None
    (* The next offset wanted by fetch. *)

  val data_received = Lwt_condition.create ()
    (* Condition signaled when the cache as [next_wanted] is filled. *)

  val mutable closed = false
    (* Whether the stream has been closed. *)

  (* Seek in the input of the cache. *)
  method seek_in offset =
    if offset <> r_offset  then
      try_lwt
        lwt pos = Lwt_unix.lseek rfd offset Unix.SEEK_SET in
        if pos <> offset then
          raise_lwt (Error "cannot seek in cache")
        else begin
          r_offset <- offset;
          return ()
        end
      with Unix.Unix_error (error, _, _) ->
        ignore (Lwt_log.error_f ~section "cannot seek in %S: %s" file (Unix.error_message error));
        raise_lwt (Error "cannot seek in cache")
    else
      return ()

  method seek_out offset =
    if offset <> w_offset  then
      try_lwt
        lwt pos = Lwt_unix.lseek wfd offset Unix.SEEK_SET in
        if pos <> offset then
          raise_lwt (Error "cannot seek in cache")
        else begin
          w_offset <- offset;
          return ()
        end
      with Unix.Unix_error (error, _, _) ->
        ignore (Lwt_log.error_f ~section "cannot seek in %S: %s" file (Unix.error_message error));
        raise_lwt (Error "cannot seek in cache")
    else
      return ()

  method close_out =
    lwt () = Lwt_unix.close wfd in
    try_lwt
      Lwt_unix.rename file (Filename.chop_extension file)
    with Unix.Unix_error (error, _, _) ->
      ignore (Lwt_log.error_f ~section "cannot rename %S to %S: %s" file (Filename.chop_extension file) (Unix.error_message error));
      return ()

  method fill_cache offset length =
    if closed then
      Lwt_unix.close wfd
    else
      let offset =
        match next_wanted with
          | Some offset' ->
              if offset' <> offset then begin
                ignore (Lwt_log.error ~section "arg");
                assert false
              end else
                offset
          | None ->
              offset
      in
      lwt () = self#seek_out offset in
      (* Fetch data from spotify at offset. *)
      lwt data = super#fetch offset in
      (* Write them to the cache. *)
      lwt () = write_string wfd data in
      w_offset <- w_offset + String.length data;
      (* Mark the block as cached. *)
      cached <- Int_set.add offset cached;
      (* Notify the fetcher. *)
      (match next_wanted with
         | Some _ ->
             next_wanted <- None;
             Lwt_condition.signal data_received ()
         | None ->
             ());
      if String.length data < block_size then
        (* End of file reached, try to filling missing part if there
           are some. *)
        match try Some (Int_map.min_binding skipped) with Not_found -> None with
          | Some (offset, length) ->
              skipped <- Int_map.remove offset skipped;
              self#fill_cache offset length
          | None ->
              self#close_out
      else if offset + block_size >= length then
        (* We have reached the zone of missing data, try filling
           missing part if there are some. *)
        match try Some (Int_map.min_binding skipped) with Not_found -> None with
          | Some (offset, length) ->
              skipped <- Int_map.remove offset skipped;
              self#fill_cache offset length
          | None ->
              self#close_out
      else
        self#fill_cache (offset + block_size) (length - block_size)

  method fetch offset =
    lwt () =
      if Int_set.mem offset cached then
        (* Data are already cached, do nothing. *)
        return ()
      else begin
        (* Wait for data to be cached. *)
        next_wanted <- Some offset;
        Lwt_condition.wait data_received
      end
    in
    lwt () = self#seek_in offset in
    (* Read data from the cache. *)
    lwt data = read_string rfd block_size in
    r_offset <- r_offset + String.length data;
    return data

  method close =
    closed <- true;
    lwt () = super#close in
    Lwt_unix.close rfd

  initializer
    ignore (self#fill_cache 0 max_int)
end

let page_stream_from_spotify session track_id file_id cache_file =
  (* Tell spotify we are playing. *)
  lwt () = send_packet session#session_parameters CMD_REQUEST_PLAY "" in
  (* Request a new key. *)
  lwt aes = request_key session track_id file_id in
  (* Try to open the cache. *)
  match_lwt Cache.get_writer session ~overwrite:true (cache_file ^ ".temp") with
    | Some (file, wfd) -> begin
        (* Do not lock the cache for streaming. *)
        Cache.release ();
        try_lwt
          (* Also open the cache file for reading. *)
          lwt rfd = Lwt_unix.openfile file [Unix.O_RDONLY] 0 in
          return (new page_stream_from_spotify_with_cache session file_id aes file rfd wfd :> page_stream)
        with Unix.Unix_error (error, _, _) ->
          ignore (Lwt_log.error_f ~section "cannot open %S for reading: %s" file (Unix.error_message error));
          return (new page_stream_from_spotify session file_id aes :> page_stream)
      end
    | None ->
        return (new page_stream_from_spotify session file_id aes :> page_stream)

(* +-----------------------------------------------------------------+
   | Streaming                                                       |
   +-----------------------------------------------------------------+ *)

let open_track session ~track_id ~file_id =
  if id_length track_id <> 16 then raise (Wrong_id "track");
  if id_length file_id <> 20 then raise (Wrong_id "file");
  let cache_file = Filename.concat "tracks" (string_of_id file_id ^ ".ogg") in
  match_lwt Cache.get_reader session cache_file with
    | Some (file, fd) ->
        (* Do not lock the cache for streaming. *)
        Cache.release ();
        create_stream (new page_stream_from_fd fd)
    | None ->
        lwt page_stream = page_stream_from_spotify session track_id file_id cache_file in
        create_stream page_stream
