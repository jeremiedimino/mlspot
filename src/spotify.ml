(*
 * spotify.ml
 * ----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

open Lwt

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

(* +-----------------------------------------------------------------+
   | Cache versions                                                  |
   +-----------------------------------------------------------------+ *)

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
    String.unsafe_set packet.buf (packet.ofs + 0) (Char.unsafe_chr (x lsr 24));
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

(* Information about a channel. *)
type channel = {
  mutable ch_data : string list;
  (* Data of the channel, in reverse order of reception. *)

  ch_wakener : string Lwt.u;
  (* Wakener for when the packet is terminated. *)
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
}

(* Sessions are objects so they are comparable and hashable. *)
class session ?(use_cache = true) ?(cache_dir = Filename.concat xdg_cache_home "mlspot") () = object
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
end

let create ?use_cache ?cache_dir () = new session ?use_cache ?cache_dir ()
let online session = session#get_session_parameters <> None

let get_use_cache session = session#get_use_cache
let set_use_cache session x = session#set_use_cache x

let get_cache_dir session = session#get_cache_dir
let set_cache_dir session x = session#set_cache_dir x

(* +-----------------------------------------------------------------+
   | Utils                                                           |
   +-----------------------------------------------------------------+ *)

(* Return a thread which fails when the session is closed. *)
let or_logout sp w =
  pick [w; sp.logout_waiter >> fail Exit]

(* +-----------------------------------------------------------------+
   | Channels                                                        |
   +-----------------------------------------------------------------+ *)

(* Allocate a channel. If all channel ID are taken, wait for one to be
   released. It returns the channel id, the packet of the channel and
   a thread which terminates when the packet is completely
   received. *)
let alloc_channel sp =
  let rec loop id =
    if Channel_map.mem id sp.channels then
      let id = (id + 1) land 0xffff in
      if id = sp.next_channel_id then
        None
      else
        loop id
    else begin
      sp.next_channel_id <- id + 1;
      let packet = Packet.create () in
      let waiter, wakener = task () in
      sp.channels <- Channel_map.add id { ch_data = []; ch_wakener = wakener } sp.channels;
      Some (id, packet, or_logout sp waiter)
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
  return (command_of_int (Char.code (String.unsafe_get header 0)), String.sub payload 0 len)

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

  let rec mkdirp dir =
    try_lwt
      lwt () = Lwt_unix.access dir [Unix.F_OK] in
      return true
    with Unix.Unix_error _ ->
      lwt ok = mkdirp (Filename.dirname dir) in
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

  let rec save session name data =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      if Sys.file_exists filename then
        return ()
      else
        try_lwt
          lwt ok = mkdirp (Filename.dirname filename) in
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

  let rec get_writer session name =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      if Sys.file_exists filename then
        return None
      else
        lwt () = acquire () in
        try_lwt
          lwt ok = mkdirp (Filename.dirname filename) in
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
   | Dispatching                                                     |
   +-----------------------------------------------------------------+ *)

let dispatch sp command payload =
  match command with
    | CMD_SECRET_BLOCK ->
        send_packet sp CMD_CACHE_HASH "\xf4\xc2\xaa\x05\xe8\x25\xa7\xb5\xe4\xe6\x59\x0f\x3d\xd0\xbe\x0a\xef\x20\x51\x95"

    | CMD_PING ->
        send_packet sp CMD_PONG "\x00\x00\x00\x00"

    | CMD_WELCOME ->
        wakeup sp.login_wakener ();
        return ()

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
                  (* Concatenates all data received. *)
                  let len = List.fold_left (fun len str -> len + String.length str - 2) 0 channel.ch_data in
                  let res = String.create len in
                  let rec loop ofs data =
                    match data with
                      | str :: data ->
                          let len = String.length str - 2 in
                          String.unsafe_blit str 2 res (ofs - len) len;
                          loop (ofs - len) data
                      | [] ->
                          (* Skip headers. *)
                          let rec skip ofs =
                            if ofs + 2 > String.length res then begin
                              wakeup_exn channel.ch_wakener (Error "invalid data received");
                              return ()
                            end else
                              let len = get_int16 res ofs in
                              let ofs = ofs + 2 in
                              if len = 0 then begin
                                (* Send the result to the channel owner. *)
                                wakeup channel.ch_wakener (String.sub res ofs (String.length res - ofs));
                                return ()
                              end else
                                skip (ofs + len)
                          in
                          skip 0
                  in
                  loop len channel.ch_data
                end else begin
                  channel.ch_data <- payload :: channel.ch_data;
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
                wakeup_exn channel.ch_wakener (Error "channel error");
                return ()
        end

    | CMD_AES_KEY ->
        (*let t = Cryptokit.Cipher.aes (String.sub payload 4 (String.length payload - 4)) Cryptokit.Cipher.Encrypt in*)
        return ()

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
  lwt () =
    try_lwt
      lwt command, payload = recv_packet sp in
      ignore (
        try_lwt
          dispatch sp command payload
        with exn ->
          Lwt_log.error ~section ~exn "dispatcher failed with"
      );
      return ()
    with
      | Unknown_command command ->
          ignore (Lwt_log.error_f ~section "unknown command received (0x%02x)" command);
          return ()
      | Logged_out | Disconnected ->
          return ()
      | End_of_file ->
          disconnect sp
      | exn ->
          ignore (Lwt_log.error ~section ~exn "command reader failed with");
          disconnect sp
  in
  loop_dispatch sp

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

    (* The secure rng is two slow, so we use a pseudo-random one. *)
    let rng = Cryptokit.Random.pseudo_rng client_random in

    (* Generate a secret. *)
    let secret = Cryptokit.DH.private_secret ~rng dh_parameters in

    (* Generate a new RSA key (TODO: use a more secure random number
       generator, the secure one is too slow). *)
    let rsa = Cryptokit.RSA.new_key ~rng ~e:65537 1024 in

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
    Packet.add_string packet rsa.Cryptokit.RSA.n;
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
   | XML                                                             |
   +-----------------------------------------------------------------+ *)

TAGS [
  "mlspot-cache-version";
  "id";
  "width";
  "height";
  "name";
  "genres";
  "years-active";
  "artist";
  "artists";
  "portrait";
  "portraits";
  "text";
  "bio";
  "bios";
  "similar-artists";
  "text";
  "catalogues";
  "forbidden";
  "restriction";
  "restrictions";
  "artist-id";
  "track";
  "tracks";
  "track-number";
  "length";
  "files";
  "popularity";
  "external-id";
  "external-ids";
  "format";
  "title";
  "file";
  "type";
  "disc-number";
  "album-type";
  "year";
  "cover";
  "copyright";
  "c";
  "p";
  "disc";
  "discs";
  "similar-artist";
  "album";
  "albums";
  "alternatives";
  "allowed";
  "explicit";
  "artist-name";
  "redirect";
  "album-id";
  "album-artist";
  "album-artist-id";
  "did-you-mean";
  "total-artists";
  "total-albums";
  "total-tracks";
  "result";
]

let rec search_tag name a b =
  if a = b then
    raise Not_found
  else
    let c = (a + b) / 2 in
    let name' = Array.unsafe_get tags c in
    match String.compare name name' with
      | n when n < 0 ->
          search_tag name a c
      | n when n > 0 ->
          search_tag name (c + 1) b
      | _ ->
          c

let make_tag name = search_tag name 0 (Array.length tags)

module Tag_map = Map.Make (struct
                             type t = int
                             let compare a b = a - b
                           end)

type data = string
    (* Type of attributes and CDATA. *)

type node = {
  mutable nodes : node list Tag_map.t;
  (* Children, by tags. Children for a given tag are in reverse
     order. *)
  mutable datas : data list Tag_map.t;
  (* Children containing only one CDATA, by tags as well as
     attributes. Datas for a given tag are in reverse order. *)
}

let empty = {
  nodes = Tag_map.empty;
  datas = Tag_map.empty;
}

let add name elt map =
  try
    let tag = make_tag name in
    try
      Tag_map.add tag (elt :: Tag_map.find tag map) map
    with Not_found ->
      Tag_map.add tag [elt] map
  with Not_found ->
    map

let get_datas tag node = try List.rev (Tag_map.find tag node.datas) with Not_found -> []
let get_nodes tag node = try List.rev (Tag_map.find tag node.nodes) with Not_found -> []

let get_data tag node =
  match Tag_map.find tag node.datas with
    | [x] -> x
    | _ -> raise (Error (Printf.sprintf "too many datas to unpack for tag '%s'" tags.(tag)))

let get_node tag node =
  match Tag_map.find tag node.nodes with
    | [x] -> x
    | _ -> raise (Error (Printf.sprintf "too many nodes to unpack for tag '%s'" tags.(tag)))

module XML = struct

  type data_maker = {
    mutable data_size : int;
    mutable data_data : string list;
  }

  module Datas = Weak.Make (struct
                              type t = data
                              let equal = ( = )
                              let hash = Hashtbl.hash
                            end)

  let data_cache = Datas.create 16384

  let empty = Datas.merge data_cache ""

  let make_data dm =
    match dm.data_data with
      | [] ->
          empty
      | [str] ->
          Datas.merge data_cache str
      | _ ->
          let res = String.create dm.data_size in
          let rec loop ofs l =
            match l with
              | [] ->
                  Datas.merge data_cache res
              | str :: l ->
                  let len = String.length str in
                  let ofs = ofs - len in
                  String.unsafe_blit str 0 res ofs len;
                  loop ofs l
          in
          loop dm.data_size dm.data_data

  type state = Undefined of string list Tag_map.t | Data of data_maker | Node of node

  let is_space str =
    let rec loop idx =
      if idx = String.length str then
        true
      else
        match String.unsafe_get str idx with
          | ' ' | '\t' | '\n' ->
              loop (idx + 1)
          | _ ->
              false
    in
    loop 0

  let rec print indent node =
    Tag_map.iter
      (fun tag elts ->
         Printf.printf "%s%s = [%s]\n" indent tags.(tag) (String.concat "; " (List.rev_map (Printf.sprintf "%S") elts)))
      node.datas;
    Tag_map.iter
      (fun tag elts ->
         List.iter
           (fun elt ->
              Printf.printf "%s%s = {\n" indent tags.(tag);
              print ("  " ^ indent) elt;
              Printf.printf "%s}\n" indent)
           (List.rev elts))
      node.nodes

  let create_parser () =
    let xp = Expat.parser_create None in
    let node = { nodes = Tag_map.empty; datas = Tag_map.empty } in
    let stack = ref [] in
    let state = ref (Node node) in
    Expat.set_start_element_handler xp
      (fun name attrs ->
         match !state with
           | Undefined datas ->
               stack := { nodes = Tag_map.empty; datas } :: !stack;
               state := Undefined (List.fold_left (fun map (key, value) -> add key (Datas.merge data_cache value) map) Tag_map.empty attrs)
           | Data str ->
               raise (Error "cannot mix element and cdata")
           | Node node ->
               stack := node :: !stack;
               state := Undefined (List.fold_left (fun map (key, value) -> add key (Datas.merge data_cache value) map) Tag_map.empty attrs));
    Expat.set_end_element_handler xp
      (fun name ->
         let node =
           match !stack with
             | [] ->
                 assert false
             | node :: rest ->
                 stack := rest;
                 node
         in
         let old_state = !state in
         state := Node node;
         match old_state with
           | Undefined datas ->
               node.nodes <- add name { nodes = Tag_map.empty; datas } node.nodes
           | Data dm ->
               node.datas <- add name (make_data dm) node.datas
           | Node node' ->
               node.nodes <- add name node' node.nodes);
    Expat.set_character_data_handler xp
      (fun str ->
         match !state with
           | Undefined datas ->
               if not (is_space str) then state := Data { data_size = String.length str; data_data = [str] }
           | Data dm ->
               dm.data_size <- dm.data_size + String.length str;
               dm.data_data <- str :: dm.data_data
           | Node _ ->
               if not (is_space str) then raise (Error "cannot mix elements and cdata"));
    (xp, node)

  let buffer_size = 8192

  module type Monad = sig
    type 'a t
    val bind : 'a t -> ('a -> 'b t) -> 'b t
    val return : 'a -> 'a t
  end

  module Make_gzip_header_reader (Lwt : Monad) = struct
    open Lwt

    let rec skip_string read_byte =
      match_lwt read_byte () with
        | 0 -> return ()
        | _ -> skip_string read_byte

    let read read_byte =
      lwt id1 = read_byte () in
      lwt id2 = read_byte () in
      if id1 <> 0x1f || id2 <> 0x8b then raise (Error "invalid gzip file: bad magic number");
      lwt cm = read_byte () in
      if cm <> 8 then raise (Error "invalid gzip file: unknown compression method");
      lwt flags = read_byte () in
      if flags land 0xe0 <> 0 then raise (Error "invalid gzip file: bad flags");
      lwt () = for_lwt i = 1 to 6 do read_byte () >> return () done in
      lwt () =
        if flags land 0x04 <> 0 then begin
          lwt len1 = read_byte () in
          lwt len2 = read_byte () in
          for_lwt i = 1 to len1 lor (len2 lsl 8) do
            lwt _ = read_byte () in
            return ()
          done
        end else
          return ()
      in
      lwt () =
        if flags land 0x08 <> 0 then
          skip_string read_byte
        else
          return ()
      in
      lwt () =
        if flags land 0x10 <> 0 then
          skip_string read_byte
        else
          return ()
      in
      if flags land 0x02 <> 0 then
        lwt _ = read_byte () in
        lwt _ = read_byte () in
        return ()
      else
        return ()
  end

  module Gzip = Make_gzip_header_reader (struct
                                           type 'a t = 'a
                                           let bind x f = f x
                                           let return x = x
                                           let fail = raise
                                         end)

  module Lwt_gzip = Make_gzip_header_reader (Lwt)

  let get_int32_little_endian str ofs =
    if ofs < 0 || ofs + 4 > String.length str then raise (Error "invalid gzip file: premature end of file");
    let x0 = Int32.of_int (Char.code (String.unsafe_get str (ofs + 0))) in
    let x1 = Int32.of_int (Char.code (String.unsafe_get str (ofs + 1))) in
    let x2 = Int32.of_int (Char.code (String.unsafe_get str (ofs + 2))) in
    let x3 = Int32.of_int (Char.code (String.unsafe_get str (ofs + 3))) in
    Int32.logor x0 (Int32.logor (Int32.shift_left x1 8) (Int32.logor (Int32.shift_left x2 16) (Int32.shift_left x3 24)))

  let parse_string str =
    let xp, node = create_parser () in
    try
      Expat.parse xp str;
      Expat.final xp;
      node
    with Expat.Expat_error error ->
      raise (Error ("invalid XML file: " ^ Expat.xml_error_to_string error))

  let parse_compressed_string str =
    let xp, node = create_parser () in
    let buffer = String.create buffer_size in
    let stream = Zlib.inflate_init false in
    try
      let rec loop ofs size crc =
        let eoi, len_src, len_dst = Zlib.inflate stream str ofs (String.length str - ofs) buffer 0 buffer_size Zlib.Z_NO_FLUSH in
        let size = Int32.add size (Int32.of_int len_dst) in
        let crc = Zlib.update_crc crc buffer 0 len_dst in
        Expat.parse_sub xp buffer 0 len_dst;
        if not eoi then
          loop (ofs + len_src) size crc
        else
          (ofs + len_src, size, crc)
      in
      let ofs = ref 0 in
      let read_byte () =
        if !ofs >= String.length str then
          raise (Error "invalid gzip file: prematire end of file")
        else begin
          let o = !ofs in
          ofs := o + 1;
          Char.code (String.unsafe_get str o)
        end
      in
      Gzip.read read_byte;
      let ofs, size, crc = loop !ofs Int32.zero Int32.zero in
      Zlib.inflate_end stream;
      let crc' = get_int32_little_endian str ofs in
      let size' = get_int32_little_endian str (ofs + 4) in
      if crc <> crc' then raise (Error "invalid gzip file: CRC mismatch");
      if size <> size' then raise (Error "invalid gzip file: size mismatch");
      Expat.final xp;
      node
    with
      | Expat.Expat_error error ->
          raise (Error ("invalid XML file: " ^ Expat.xml_error_to_string error))
      | Zlib.Error (func, msg) ->
          raise (Error ("invalid compressed data: " ^ msg))

  let parse_file_descr fd =
    let xp, node = create_parser () in
    let buffer = String.create buffer_size in
    try_lwt
      let rec loop () =
        match_lwt Lwt_unix.read fd buffer 0 buffer_size with
          | 0 ->
              return ()
          | n ->
              Expat.parse_sub xp buffer 0 n;
              loop ()
      in
      lwt () = loop () in
      Expat.final xp;
      return node
    with
      | Expat.Expat_error error ->
          raise_lwt (Error ("invalid XML file: " ^ Expat.xml_error_to_string error))
      | Unix.Unix_error (error, _, _) ->
          raise_lwt (Error ("cannot read file: " ^ Unix.error_message error))
    finally
      Lwt_unix.close fd

  let parse_compressed_file_descr fd =
    let xp, node = create_parser () in
    let ibuffer = String.create buffer_size in
    let obuffer = String.create buffer_size in
    let stream = Zlib.inflate_init false in
    try_lwt
      let rec loop ofs size crc =
        match_lwt Lwt_unix.read fd ibuffer ofs (buffer_size - ofs) with
          | 0 ->
              let rec loop ofs' size crc =
                let eoi, len_src, len_dst = Zlib.inflate stream ibuffer ofs' (ofs - ofs') obuffer 0 buffer_size Zlib.Z_NO_FLUSH in
                let size = Int32.add size (Int32.of_int len_dst) in
                let crc = Zlib.update_crc crc obuffer 0 len_dst in
                Expat.parse_sub xp obuffer 0 len_dst;
                if not eoi then
                  loop (ofs' + len_src) size crc
                else begin
                  let rem = ofs - (ofs' + len_src) in
                  if rem <> 0 then String.blit ibuffer (ofs' + len_src) ibuffer 0 rem;
                  return (rem, size, crc)
                end
              in
              loop 0 size crc
          | n ->
              let eoi, len_src, len_dst = Zlib.inflate stream ibuffer 0 (ofs + n) obuffer 0 buffer_size Zlib.Z_NO_FLUSH in
              let size = Int32.add size (Int32.of_int len_dst) in
              let crc = Zlib.update_crc crc obuffer 0 len_dst in
              Expat.parse_sub xp obuffer 0 len_dst;
              if len_src < n then begin
                String.blit ibuffer len_src ibuffer 0 (n - len_src);
                loop (n - len_src) size crc
              end else
                loop 0 size crc
      in
      let ofs = ref 0 and len = ref 0 in
      let read_byte () =
        if !ofs = !len then
          match_lwt Lwt_unix.read fd ibuffer 0 buffer_size with
            | 0 ->
                raise_lwt (Error "invalid gzip file: premature end of file")
            | n ->
                ofs := 1;
                len := n;
                return (Char.code (String.unsafe_get ibuffer 0))
        else begin
          let o = !ofs in
          ofs := o + 1;
          return (Char.code (String.unsafe_get ibuffer o))
        end
      in
      lwt () = Lwt_gzip.read read_byte in
      if !ofs < !len then String.blit ibuffer !ofs ibuffer 0 (!len - !ofs);
      lwt ofs, size, crc = loop (!len - !ofs) Int32.zero Int32.zero in
      Zlib.inflate_end stream;
      let rec refill8 ofs =
        if ofs < 8 then
          match_lwt Lwt_unix.read fd ibuffer ofs (8 - ofs) with
            | 0 -> raise_lwt (Error "invalid gzip file: premature end of file")
            | n -> refill8 (ofs + n)
        else
          return ()
      in
      lwt () = refill8 ofs in
      let crc' = get_int32_little_endian ibuffer 0 in
      let size' = get_int32_little_endian ibuffer 4 in
      if crc <> crc' then raise (Error "invalid gzip file: CRC mismatch");
      if size <> size' then raise (Error "invalid gzip file: size mismatch");
      Expat.final xp;
      return node
    with
      | Expat.Expat_error error ->
          raise_lwt (Error ("invalid XML file: " ^ Expat.xml_error_to_string error))
      | Unix.Unix_error (error, _, _) ->
          raise_lwt (Error ("cannot read file: " ^ Unix.error_message error))
    finally
      Lwt_unix.close fd

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
    Lwt_unix.close oc.fd

  let abort oc =
    lwt () = try_lwt Lwt_unix.close oc.fd with _ -> return () in
    try_lwt
      Lwt_unix.unlink oc.file
    with Unix.Unix_error (error, _, _) ->
      ignore (Lwt_log.error_f ~section "cannot unlink %S: %s" oc.file (Unix.error_message error));
      return ()

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
    lwt () = write_string oc tags.(tag) in
    lwt () = write_string oc ">" in
    lwt () = write_cdata oc data 0 0 in
    lwt () = write_string oc "</" in
    lwt () = write_string oc tags.(tag) in
    lwt () = write_string oc ">\n" in
    return ()

  let write_node oc tag f =
    lwt () = write_indent oc 0 in
    lwt () = write_string oc "<" in
    lwt () = write_string oc tags.(tag) in
    lwt () = write_string oc ">\n" in
    oc.indent <- oc.indent + 1;
    lwt () = f oc in
    oc.indent <- oc.indent - 1;
    lwt () = write_indent oc 0 in
    lwt () = write_string oc "</" in
    lwt () = write_string oc tags.(tag) in
    lwt () = write_string oc ">\n" in
    return ()

  let write_nodes oc tag writer l =
    write_node oc tag (fun oc -> Lwt_list.iter_s (fun x -> writer oc x) l)
end

(* +-----------------------------------------------------------------+
   | Cache                                                           |
   +-----------------------------------------------------------------+ *)

module Weak_cache : sig
  type 'a t
    (* Type of object catching element of type ['a], indexed by an
       ID. Element must be finalisable. *)

  val create : int -> 'a t
    (* Create a new empty cache. *)

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
  module Table = Hashtbl.Make (struct
                                 type t = id
                                 let equal = ( == )
                                 let hash = id_hash
                               end)

  type 'a t = 'a Weak.t Table.t

  let create size = Table.create size

  let finalise cache id obj =
    Table.remove cache id

  let find cache id make =
    try
      let weak = Table.find cache id in
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
      Table.add cache id weak;
      Gc.finalise (finalise cache id) data;
      data

  let find_lwt cache id make =
    try
      let weak = Table.find cache id in
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
      Table.add cache id weak;
      Gc.finalise (finalise cache id) data;
      return data

  let find_multiple_lwt cache ids make =
    let old_datas, ids =
      split_either
        (List.map
           (fun id ->
              try
                match Weak.get (Table.find cache id) 0 with
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
         Table.add cache data#id weak;
         Gc.finalise (finalise cache data#id) data)
      new_datas;
    return (old_datas @ new_datas)
end

(* +-----------------------------------------------------------------+
   | Documents                                                       |
   +-----------------------------------------------------------------+ *)

class portrait node (id : id) = object
  val width = int_of_string (get_data tag_width node)
  val height = int_of_string (get_data tag_height node)
  method id = id
  method link = Image id
  method width = width
  method height = height
end

let write_portrait oc portrait =
  XML.write_node oc tag_portrait
    (fun oc ->
       lwt () = XML.write_data oc tag_id (string_of_id portrait#id) in
       lwt () = XML.write_data oc tag_width (string_of_int portrait#width) in
       lwt () = XML.write_data oc tag_height (string_of_int portrait#height) in
       return ())

let portraits = Weak_cache.create 1024

let make_portrait node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find portraits id (fun () -> new portrait node id)

let make_portrait_option node =
  if Tag_map.is_empty node.datas then
    None
  else begin
    let id = id_of_string (get_data tag_id node) in
    Some (Weak_cache.find portraits id (fun () -> new portrait node id))
  end

class biography node = object
  val text = get_data tag_text node
  val portraits = List.map make_portrait (get_nodes tag_portrait (get_node tag_portraits node))
  method text = text
  method portraits = portraits
end

let write_biography oc bio =
  XML.write_node oc tag_bio
    (fun oc ->
       lwt () = XML.write_data oc tag_text bio#text in
       lwt () = XML.write_nodes oc tag_portraits write_portrait bio#portraits in
       return ())

class restriction node = object
  val catalogues = split ',' (get_data tag_catalogues node)
  val forbidden = try split_space_coma (get_data tag_forbidden node) with Not_found -> []
  val allowed = try split_space_coma (get_data tag_allowed node) with Not_found -> []
  method catalogues = catalogues
  method forbidden = forbidden
  method allowed = allowed
end

let write_restriction oc restriction =
  XML.write_node oc tag_restriction
    (fun oc ->
       lwt () = XML.write_data oc tag_catalogues (String.concat "," restriction#catalogues) in
       lwt () = XML.write_data oc tag_forbidden (String.concat "," restriction#forbidden) in
       lwt () = XML.write_data oc tag_allowed (String.concat "," restriction#allowed) in
       return ())

class similar_artist node (id : id) = object
  val name = get_data tag_name node
  val portrait = id_of_string (get_data tag_portrait node)
  val genres = split ',' (get_data tag_genres node)
  val years_active = List.map int_of_string (split ',' (get_data tag_years_active node));
  val restrictions = try List.map (fun node -> new restriction node) (get_nodes tag_restriction (get_node tag_restrictions node)) with Not_found -> []
  method id = id
  method link = Artist id
  method name = name
  method portrait = portrait
  method genres = genres
  method years_active = years_active
  method restrictions = restrictions
end

let write_similar_artist oc similar_artist =
  XML.write_node oc tag_similar_artist
    (fun oc ->
       lwt () = XML.write_data oc tag_id (string_of_id similar_artist#id) in
       lwt () = XML.write_data oc tag_name similar_artist#name in
       lwt () = XML.write_data oc tag_portrait (string_of_id similar_artist#portrait) in
       lwt () = XML.write_data oc tag_genres (String.concat "," similar_artist#genres) in
       lwt () = XML.write_data oc tag_years_active (String.concat "," (List.map string_of_int similar_artist#years_active)) in
       lwt () = XML.write_nodes oc tag_restrictions write_restriction similar_artist#restrictions in
       return ())

let similar_artists = Weak_cache.create 1024

let make_similar_artist node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find similar_artists id (fun () -> new similar_artist node id)

class file node (id : id) = object
  val format =
    match split ',' (get_data tag_format node) with
      | format :: bitrate :: _ ->
          (format, int_of_string bitrate)
      | _ ->
          raise (Error "invalid file format")
  method id = id
  method format = fst format
  method bitrate = snd format
end

let write_file oc file =
  XML.write_node oc tag_file
    (fun oc ->
       lwt () = XML.write_data oc tag_id (string_of_id file#id) in
       lwt () = XML.write_data oc tag_format (Printf.sprintf "%s,%d" file#format file#bitrate) in
       return ())

let files = Weak_cache.create 1024

let make_file node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find files id (fun () -> new file node id)

class alternative node (id : id) = object
  val files = List.map make_file (get_nodes tag_file (get_node tag_files node))
  val restrictions = try List.map (fun node -> new restriction node) (get_nodes tag_restriction (get_node tag_restrictions node)) with Not_found -> []
  method id = id
  method link = Track id
  method files = files
  method restrictions = restrictions
end

let write_alternative oc alternative =
  XML.write_node oc tag_track
    (fun oc ->
       lwt () = XML.write_data oc tag_id (string_of_id alternative#id) in
       lwt () = XML.write_nodes oc tag_files write_file alternative#files in
       lwt () = XML.write_nodes oc tag_restrictions write_restriction alternative#restrictions in
       return ())

let alternatives = Weak_cache.create 1024

let make_alternative node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find alternatives id (fun () -> new alternative node id)

let make_external_id node =
  (get_data tag_type node, get_data tag_id node)

let write_external_id oc (t, i) =
  XML.write_node oc tag_external_id
    (fun oc ->
       lwt () = XML.write_data oc tag_type t in
       lwt () = XML.write_data oc tag_id i in
       return ())

class track_common node (id : id) = object
  val title = get_data tag_title node
  val explicit = try bool_of_string (get_data tag_explicit node) with Not_found -> false
  val artists = get_datas tag_artist node
  val artists_id = List.map id_of_string (get_datas tag_artist_id node)
  val number = int_of_string (get_data tag_track_number node)
  val length = float (int_of_string (get_data tag_length node)) /. 1000.
  val files = List.map make_file (get_nodes tag_file (get_node tag_files node))
  val popularity = float_of_string (get_data tag_popularity node)
  val external_ids = List.map (fun node -> make_external_id node) (get_nodes tag_external_id (get_node tag_external_ids node))
  val alternatives = try List.map make_alternative (get_nodes tag_track (get_node tag_alternatives node)) with Not_found -> []
  method id = id
  method link = Track id
  method title = title
  method explicit = explicit
  method artists = artists
  method artists_id = artists_id
  method number = number
  method length = length
  method files = files
  method popularity = popularity
  method external_ids = external_ids
  method alternatives = alternatives
end

class track node (id : id) = object
  inherit track_common node id
  val album = get_data tag_album node
  val album_id = id_of_string (get_data tag_album_id node)
  val album_artist = get_data tag_album_artist node
  val album_artist_id = id_of_string (get_data tag_album_artist_id node)
  val cover = id_of_string (get_data tag_cover node)
  val year = int_of_string (get_data tag_year node)
  method album = album
  method album_id = album_id
  method album_artist = album_artist
  method album_artist_id = album_artist_id
  method cover = cover
  method year = year
end

type album_temp = {
  at_name : string;
  at_id : id;
  at_artist : string;
  at_artist_id : id;
  at_cover : id;
  at_year : int;
}

class track_from_album node id album = object
  inherit track_common node id
  method album = album.at_name
  method album_id = album.at_id
  method album_artist = album.at_artist
  method album_artist_id = album.at_artist_id
  method cover = album.at_cover
  method year = album.at_year
end

let write_track oc track =
  XML.write_node oc tag_track
    (fun oc ->
       lwt () = XML.write_data oc tag_mlspot_cache_version (string_of_int track_cache_version) in
       lwt () = XML.write_data oc tag_id (string_of_id track#id) in
       lwt () = XML.write_data oc tag_title track#title in
       lwt () = XML.write_data oc tag_explicit (string_of_bool track#explicit) in
       lwt () = Lwt_list.iter_s (XML.write_data oc tag_artist) track#artists in
       lwt () = Lwt_list.iter_s (XML.write_data oc tag_artist_id) (List.map string_of_id track#artists_id) in
       lwt () = XML.write_data oc tag_album track#album in
       lwt () = XML.write_data oc tag_album_id (string_of_id track#album_id) in
       lwt () = XML.write_data oc tag_album_artist track#album_artist in
       lwt () = XML.write_data oc tag_album_artist_id (string_of_id track#album_artist_id) in
       lwt () = XML.write_data oc tag_track_number (string_of_int track#number) in
       lwt () = XML.write_data oc tag_length (string_of_int (int_of_float (track#length *. 1000.))) in
       lwt () = XML.write_nodes oc tag_files write_file track#files in
       lwt () = XML.write_data oc tag_cover (string_of_id track#cover) in
       lwt () = XML.write_data oc tag_year (string_of_int track#year) in
       lwt () = XML.write_data oc tag_popularity (string_of_float track#popularity) in
       lwt () = XML.write_nodes oc tag_external_ids write_external_id track#external_ids in
       lwt () = XML.write_nodes oc tag_alternatives write_alternative track#alternatives in
       return ())

let tracks = Weak_cache.create 1024

let make_track node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find tracks id (fun () -> new track node id)

let make_track_from_album album node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find tracks id (fun () -> new track_from_album node id album)

class disc_common node = object
  val number = int_of_string (get_data tag_disc_number node)
  val name = try Some (get_data tag_name node) with Not_found -> None
  method number = number
  method name = name
end

class disc node album = object
  inherit disc_common node
  val tracks = List.map (make_track_from_album album) (get_nodes tag_track node)
  method tracks = tracks
end

let disc_from_cache node get_tracks =
  lwt tracks = get_tracks (List.map id_of_string (get_datas tag_track node)) in
  return (object
            inherit disc_common node
            method tracks = tracks
          end)

let write_track_id oc track =
  XML.write_data oc tag_track (string_of_id track#id)

let write_disc oc disc =
  XML.write_node oc tag_disc
    (fun oc ->
       lwt () = XML.write_data oc tag_disc_number (string_of_int disc#number) in
       lwt () = XML.write_data oc tag_name (match disc#name with Some name -> name | None -> "") in
       lwt () = XML.write_nodes oc tag_tracks write_track_id disc#tracks in
       return ())

let album_temp node id = {
  at_name = get_data tag_name node;
  at_id = id;
  at_artist = get_data tag_artist node;
  at_artist_id = id_of_string (get_data tag_artist_id node);
  at_cover = id_of_string (get_data tag_cover node);
  at_year = int_of_string (get_data tag_year node);
}

class album_common node at (id : id) = object
  val name = at.at_name
  val artist = at.at_artist
  val artist_id = at.at_artist_id
  val album_type = get_data tag_album_type node
  val year = at.at_year
  val cover = at.at_cover
  val copyrights =
    let node = get_node tag_copyright node in
    List.map (fun c -> ("c", c)) (get_datas tag_c node) @ List.map (fun c -> ("p", c)) (get_datas tag_p node)
  val restrictions = try List.map (fun node -> new restriction node) (get_nodes tag_restriction (get_node tag_restrictions node)) with Not_found -> []
  val external_ids = try List.map (fun node -> make_external_id node) (get_nodes tag_external_id (get_node tag_external_ids node)) with Not_found -> []
  method id = id
  method link = Album id
  method name = name
  method artist = artist
  method artist_id = artist_id
  method album_type = album_type
  method year = year
  method cover = cover
  method copyrights = copyrights
  method restrictions = restrictions
  method external_ids = external_ids
end

class album node (id : id) =
  let at = album_temp node id in
object
  inherit album_common node at id
  val discs = List.map (fun node -> new disc node at) (get_nodes tag_disc (get_node tag_discs node))
  method discs = discs
end

let album_from_cache node get_tracks =
  lwt discs = Lwt_list.map_p (fun node -> disc_from_cache node get_tracks) (get_nodes tag_disc (get_node tag_discs node)) in
  let id = id_of_string (get_data tag_id node) in
  return (object
            inherit album_common node (album_temp node id) id
            method discs = discs
          end)

let write_copyright oc (kind, data) =
  XML.write_data oc
    (match kind with
       | "c" -> tag_c
       | "p" -> tag_p
       | _ -> assert false)
    data

let write_album oc album =
  XML.write_node oc tag_album
    (fun oc ->
       lwt () = XML.write_data oc tag_mlspot_cache_version (string_of_int album_cache_version) in
       lwt () = XML.write_data oc tag_id (string_of_id album#id) in
       lwt () = XML.write_data oc tag_name album#name in
       lwt () = XML.write_data oc tag_artist album#artist in
       lwt () = XML.write_data oc tag_artist_id (string_of_id album#artist_id) in
       lwt () = XML.write_data oc tag_album_type album#album_type in
       lwt () = XML.write_data oc tag_year (string_of_int album#year) in
       lwt () = XML.write_data oc tag_cover (string_of_id album#cover) in
       lwt () = XML.write_nodes oc tag_copyright write_copyright album#copyrights in
       lwt () = XML.write_nodes oc tag_restrictions write_restriction album#restrictions in
       lwt () = XML.write_nodes oc tag_external_ids write_external_id album#external_ids in
       lwt () = XML.write_nodes oc tag_discs write_disc album#discs in
       return ())

let albums = Weak_cache.create 1024

let make_album node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find albums id (fun () -> new album node id)

class artist_common node (id : id) = object
  val name = get_data tag_name node
  val portrait = make_portrait (get_node tag_portrait node)
  val biographies = List.map (fun node -> new biography node) (get_nodes tag_bio (get_node tag_bios node))
  val similar_artists = List.map make_similar_artist (get_nodes tag_similar_artist (get_node tag_similar_artists node))
  val genres = split ',' (get_data tag_genres node)
  val years_active = List.map int_of_string (split ',' (get_data tag_years_active node))
  method id = id
  method link = Artist id
  method name = name
  method portrait = portrait
  method biographies = biographies
  method similar_artists = similar_artists
  method genres = genres
  method years_active = years_active
end

class artist node (id : id) = object
  inherit artist_common node id
  val albums = List.map make_album (get_nodes tag_album (get_node tag_albums node))
  method albums = albums
end

let artist_from_cache node get_album =
  lwt albums = Lwt_list.map_p (fun str -> get_album (id_of_string str)) (get_datas tag_album (get_node tag_albums node)) in
  let id = id_of_string (get_data tag_id node) in
  return (object
            inherit artist_common node id
            method albums = albums
          end)

let write_album_id oc album =
  XML.write_data oc tag_album (string_of_id album#id)

let write_artist oc artist =
  XML.write_node oc tag_artist
    (fun oc ->
       lwt () = XML.write_data oc tag_mlspot_cache_version (string_of_int artist_cache_version) in
       lwt () = XML.write_data oc tag_id (string_of_id artist#id) in
       lwt () = XML.write_data oc tag_name artist#name in
       lwt () = write_portrait oc artist#portrait in
       lwt () = XML.write_nodes oc tag_bios write_biography artist#biographies in
       lwt () = XML.write_nodes oc tag_similar_artists write_similar_artist artist#similar_artists in
       lwt () = XML.write_data oc tag_genres (String.concat "," artist#genres) in
       lwt () = XML.write_data oc tag_years_active (String.concat "," (List.map string_of_int artist#years_active)) in
       lwt () = XML.write_nodes oc tag_albums write_album_id artist#albums in
       return ())

let artists = Weak_cache.create 1024

let make_artist node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find artists id (fun () -> new artist node id)

class artist_search node (id : id) = object
  val name = get_data tag_name node
  val portrait = make_portrait_option (get_node tag_portrait node)
  val popularity = float_of_string (get_data tag_popularity node)
  val restrictions = try List.map (fun node -> new restriction node) (get_nodes tag_restriction (get_node tag_restrictions node)) with Not_found -> []
  method id = id
  method link = Artist id
  method name = name
  method portrait = portrait
  method popularity = popularity
  method restrictions = restrictions
end

let artist_searchs = Weak_cache.create 1024

let make_artist_search node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find artist_searchs id (fun () -> new artist_search node id)

class album_search node (id : id) = object
  val name = get_data tag_name node
  val artist = get_data tag_artist_name node
  val artist_id = id_of_string (get_data tag_artist_id node)
  val cover = id_of_string (get_data tag_cover node)
  val popularity = float_of_string (get_data tag_popularity node)
  val restrictions = try List.map (fun node -> new restriction node) (get_nodes tag_restriction (get_node tag_restrictions node)) with Not_found -> []
  val external_ids = try List.map (fun node -> make_external_id node) (get_nodes tag_external_id (get_node tag_external_ids node)) with Not_found -> []
  method id = id
  method link = Album id
  method name = name
  method artist = artist
  method artist_id = artist_id
  method cover = cover
  method popularity = popularity
  method restrictions = restrictions
  method external_ids = external_ids
end

let album_searchs = Weak_cache.create 1024

let make_album_search node =
  let id = id_of_string (get_data tag_id node) in
  Weak_cache.find album_searchs id (fun () -> new album_search node id)

class search_result query node = object
  val did_you_mean = try Some (get_data tag_did_you_mean node) with Not_found -> None
  val total_artists = int_of_string (get_data tag_total_artists node)
  val total_albums = int_of_string (get_data tag_total_albums node)
  val total_tracks = int_of_string (get_data tag_total_tracks node)
  val artists = List.map make_artist_search (get_nodes tag_artist (get_node tag_artists node))
  val albums = List.map make_album_search (get_nodes tag_album (get_node tag_albums node))
  val tracks = List.map make_track (get_nodes tag_track (get_node tag_tracks node))
  method link = Search query
  method did_you_mean = did_you_mean
  method total_artists = total_artists
  method total_albums = total_albums
  method total_tracks = total_tracks
  method artists = artists
  method albums = albums
  method tracks = tracks
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
        lwt () =
          try_lwt
            lwt () = write_album oc album in
            XML.close oc
          with exn ->
            ignore (Lwt_log.error_f ~section ~exn "failed to save metadata for album %S" (string_of_id album#id));
            XML.abort oc
          finally
            Cache.release ();
            return ()
        in
        Lwt_list.iter_p (fun disc -> Lwt_list.iter_p (save_track session) disc#tracks) album#discs

let save_artist session artist =
  match_lwt Cache.get_writer session (make_filename ["metadata"; "artists"; (string_of_id artist#id ^ ".xml.gz")]) with
    | None ->
        return ()
    | Some (file, fd) ->
        let oc = XML.create_writer fd file in
        lwt () =
          try_lwt
            lwt () = write_artist oc artist in
            XML.close oc
          with exn ->
            ignore (Lwt_log.error_f ~section ~exn "failed to save metadata for artist %S" (string_of_id artist#id));
            XML.abort oc
          finally
            Cache.release ();
            return ()
        in
        Lwt_list.iter_p (save_album session) artist#albums

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

(* +-----------------------------------------------------------------+
   | Commands                                                        |
   +-----------------------------------------------------------------+ *)

let safe_parse name f =
  try
    f ()
  with exn ->
    ignore (Lwt_log.error_f ~section ~exn "failed to parse %s XML" name);
    raise (Error (Printf.sprintf "failed to parse %s XML" name))

type fetch_kind = Cache | Fresh

let safe_unlink file =
  try_lwt
    Lwt_unix.unlink file
  with Unix.Unix_error (error, _, _) ->
    ignore (Lwt_log.error_f ~section "cannot remove %S: %s" file (Unix.error_message error));
    return ()


let rec browse session ids kind kind_id root cache_version =
  if List.exists (fun id -> id_length id <> 16) ids then raise (Wrong_id kind);
  lwt nodes =
    Lwt_list.map_p
      (fun id ->
         let filename = make_filename ["metadata"; kind ^ "s"; string_of_id id ^ ".xml.gz"] in
         match_lwt Cache.get_reader session filename with
           | None ->
               return (Inr id)
           | Some (file, fd) ->
               try_lwt
                 lwt node = XML.parse_compressed_file_descr fd in
                 try
                   let node = get_node root node in
                   let cache_version' = int_of_string (get_data tag_mlspot_cache_version node) in
                   if cache_version <> cache_version' then begin
                     ignore (Lwt_log.warning_f ~section "the cache file %S does not match the version of mlspot, removing it" filename);
                     lwt () = safe_unlink file in
                     return (Inr id)
                   end else
                     return (Inl node)
                 with _ ->
                   ignore (Lwt_log.warning_f ~section "the cache file %S does not contains valid data, removing it" filename);
                   lwt () = safe_unlink file in
                   return (Inr id)
               with exn ->
                 ignore (Lwt_log.warning_f ~section ~exn "failed to read %S from the cache, removing it" filename);
                 lwt () = safe_unlink file in
                 return (Inr id)
               finally
                 Cache.release ();
                 return ())
      ids
  in
  Lwt_list.map_p
    (function
       | Inl node ->
           return (Cache, node)
       | Inr id ->
           lwt channel_id, channel_packet, channel_waiter = alloc_channel session#session_parameters in
           try_lwt
             let packet = Packet.create () in
             Packet.add_int16 packet channel_id;
             Packet.add_int8 packet kind_id;
             List.iter (fun id -> Packet.add_string packet (ID.to_bytes id)) ids;
             if kind_id = 1 || kind_id = 2 then Packet.add_int32 packet 0;
             lwt () = send_packet session#session_parameters CMD_BROWSE (Packet.contents packet) in
             lwt data = channel_waiter in
             return (Fresh, XML.parse_compressed_string data)
           finally
             release_channel session#session_parameters channel_id;
             return ())
    nodes

and get_artist session id =
  Weak_cache.find_lwt artists id
    (fun () ->
       match_lwt browse session [id] "artist" 1 tag_artist artist_cache_version with
         | [(Fresh, node)] ->
             let artist = safe_parse "artist" (fun () -> make_artist (get_node tag_artist node)) in
             delay (save_artist session artist);
             return artist
         | [(Cache, node)] ->
             safe_parse "artist" (fun () -> artist_from_cache node (get_album session))
         | _ ->
             assert false)

and get_album session id =
  Weak_cache.find_lwt albums id
    (fun () ->
       match_lwt browse session [id] "album" 2 tag_album album_cache_version with
         | [(Fresh, node)] ->
             let album = safe_parse "album" (fun () -> make_album (get_node tag_album node)) in
             delay (save_album session album);
             return album
         | [(Cache, node)] ->
             safe_parse "album" (fun () -> album_from_cache node (get_tracks session))
         | _ ->
             assert false)

and get_tracks session ids =
  Weak_cache.find_multiple_lwt tracks ids
    (fun ids ->
       lwt nodes = browse session ids "track" 3 tag_track track_cache_version in
       return
         (List.flatten
            (List.map
              (function
                 | (Fresh, node) ->
                     let tracks = safe_parse "track" (fun () -> List.map make_track (get_nodes tag_track (get_node tag_tracks (get_node tag_result node)))) in
                     delay (Lwt_list.iter_s (save_track session) tracks);
                     tracks
                 | (Cache, node) ->
                     [safe_parse "track" (fun () ->
                                            let id = id_of_string (get_data tag_id node) in
                                            new track node id)])
              nodes)))

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
             lwt channel_id, channel_packet, channel_waiter = alloc_channel session#session_parameters in
             try_lwt
               let packet = Packet.create () in
               Packet.add_int16 packet channel_id;
               Packet.add_string packet (ID.to_bytes id);
               lwt () = send_packet session#session_parameters CMD_IMAGE (Packet.contents packet) in
               lwt data = channel_waiter in
               delay (Cache.save session filename data);
               return data)

let search session ?(offset = 0) ?(length = 1000) query =
  let len = String.length query in
  if len > 255 then invalid_arg "Spotify.search";
  lwt channel_id, channel_packet, channel_waiter = alloc_channel session#session_parameters in
  try_lwt
    let packet = Packet.create () in
    Packet.add_int16 packet channel_id;
    Packet.add_int32 packet offset;
    Packet.add_int32 packet length;
    Packet.add_int16 packet 0;
    Packet.add_int8 packet len;
    Packet.add_string packet query;
    lwt () = send_packet session#session_parameters CMD_SEARCH (Packet.contents packet) in
    lwt data = channel_waiter in
    let search = safe_parse "search result" (fun () -> new search_result query (get_node tag_result (XML.parse_compressed_string data))) in
    delay (Lwt_list.iter_p (save_track session) search#tracks);
    return search
  finally
    release_channel session#session_parameters channel_id;
    return ()

(*
let fetch session track_id file_id =
  if id_length track_id <> 16 || id_length file_id <> 20 then raise Invalid_id_length;
  lwt () = send_packet session#get CMD_REQUEST_PLAY "" in
  let packet = Packet.create () in
  Packet.add_string packet file_id;
  Packet.add_string packet track_id;
  Packet.add_int16 packet 0;
  Packet.add_int16 packet 0;
  lwt () = send_packet session#get CMD_REQUEST_KEY (Packet.contents packet) in
  lwt () = Lwt_unix.sleep 1. in
  Packet.reset packet;
  Packet.add_int16 packet 1;
  Packet.add_int16 packet 0x0800;
  Packet.add_int16 packet 0x0000;
  Packet.add_int16 packet 0x0000;
  Packet.add_int16 packet 0x0000;
  Packet.add_int16 packet 0x4e20;
  Packet.add_int32 packet (200 * 1000);
  Packet.add_string packet file_id;
  Packet.add_int32 packet 0;
  Packet.add_int32 packet (100000 * 4096 / 4);
  lwt () = send_packet session#get CMD_GET_DATA (Packet.contents packet) in
  return ()
*)
