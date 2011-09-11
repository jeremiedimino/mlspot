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

let xdg_cache_home =
  try
    Sys.getenv "XDG_CACHE_HOME"
  with Not_found ->
    let home = try Sys.getenv "HOME" with Not_found -> (Unix.getpwuid (Unix.getuid ())).Unix.pw_dir in
    if Sys.os_type = "Win32" then
      Filename.concat (Filename.concat home "Local Settings") "Cache"
    else
      Filename.concat home ".cache"

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
  let load session name =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      try_lwt
        Lwt_io.with_file ~mode:Lwt_io.input filename Lwt_io.read >|= fun x -> Some x
      with exn ->
        ignore (Lwt_log.debug_f ~exn "failed to load %S from the cache" name);
        return None
    else
      return None

  let save session name data =
    if session#get_use_cache then
      let filename = Filename.concat session#get_cache_dir name in
      try_lwt
        let rec loop dir =
          try_lwt
            Lwt_unix.access dir [Unix.F_OK]
          with Unix.Unix_error _ ->
            lwt () = loop (Filename.dirname dir) in
            Lwt_unix.mkdir dir 0o755
        in
        lwt () = loop (Filename.dirname filename) in
        Lwt_io.with_file ~mode:Lwt_io.output filename (fun oc -> Lwt_io.write oc data)
      with exn ->
        ignore (Lwt_log.error_f ~exn "failed to save %S to the cache" name);
        return ()
    else
      return ()
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
        Lwt_log.warning_f ~section "do not know what to do with command '%s'" (string_of_command command)

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
      match server_random.[1] with
        | '\x01' -> raise (Authentication_failure "client upgrade recquired")
        | '\x03' -> raise (Authentication_failure "user not found")
        | '\x04' -> raise (Authentication_failure "account has been disabled")
        | '\x06' -> raise (Authentication_failure "you need to complete your account details")
        | '\x09' -> raise (Authentication_failure "country mismatch")
        | _ -> raise (Authentication_failure "unknown error")
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

type id = string

exception Id_parse_failure
exception Wrong_id of string

let id_length = String.length

let int_of_hexa = function
  | '0' .. '9' as ch -> Char.code ch - Char.code '0'
  | 'a' .. 'f' as ch -> Char.code ch - Char.code 'a' + 10
  | 'A' .. 'F' as ch -> Char.code ch - Char.code 'A' + 10
  | _ -> raise Id_parse_failure

let id_of_string str =
  let len = String.length str in
  if len land 1 <> 0 then raise Id_parse_failure;
  let id = String.create (len / 2) in
  for i = 0 to len / 2 - 1 do
    let x0 = int_of_hexa (String.unsafe_get str (i * 2 + 0)) in
    let x1 = int_of_hexa (String.unsafe_get str (i * 2 + 1)) in
    String.unsafe_set id i (Char.unsafe_chr ((x0 lsl 4) lor x1))
  done;
  id

let hexa_of_int n =
  if n < 10 then
    Char.unsafe_chr (Char.code '0' + n)
  else
    Char.unsafe_chr (Char.code 'a' + n - 10)

let string_of_id id =
  let len = String.length id in
  let str = String.create (len * 2) in
  for i = 0 to len - 1 do
    let x = Char.code (String.unsafe_get id i) in
    String.unsafe_set str (i * 2 + 0) (hexa_of_int (x lsr 4));
    String.unsafe_set str (i * 2 + 1) (hexa_of_int (x land 15))
  done;
  str

(* +-----------------------------------------------------------------+
   | Enumerations                                                    |
   +-----------------------------------------------------------------+ *)

exception Out_of_bounds

class ['a] enum arr map = object
  method length = Array.length arr

  method get idx =
    if idx < 0 || idx >= Array.length arr then raise Out_of_bounds;
    map (Array.unsafe_get arr idx)

  method to_list =
    let rec loop idx acc =
      if idx = -1 then
        acc
      else
        loop (idx - 1) (map (Array.unsafe_get arr idx) :: acc)
    in
    loop (Array.length arr) []

  method to_array =
    Array.map map arr

  method iter (f : 'a -> unit) =
    for idx = 0 to Array.length arr - 1 do
      f (map (Array.unsafe_get arr idx))
    done

  method fold : 'b. ('a -> 'b -> 'b) -> 'b -> 'b = fun f acc ->
    let acc = ref acc in
    for idx = 0 to Array.length arr - 1 do
      acc := f (map (Array.unsafe_get arr idx)) !acc
    done;
    !acc
end

(* +-----------------------------------------------------------------+
   | XML                                                             |
   +-----------------------------------------------------------------+ *)

module Tag = struct
  (* We create a big weak table of all encountered tags in order to
     save memory: two equal tags are represented by the same physical
     string. *)
  module Tags = Weak.Make (struct
                             type t = string
                             let equal = ( == )
                             let hash = Hashtbl.hash
                           end)

  let tags = Tags.create 16384

  let make tag = Tags.merge tags tag
end

module Tag_map = Map.Make (struct
                             type t = string
                             let compare a b =
                               if a == b then
                                 0
                               else
                                 String.compare a b
                           end)

type data = string
    (* Type of attributes and PCDATA. *)

type node = {
  nodes : (string * node array) array;
  (* Sorted array of children which contains XML nodes. *)
  datas : (string * data array) array;
  (* Sorted array of data. It is either attributes of the node, or
     childlren containing only one pcdata. *)
}

let empty = {
  nodes = [||];
  datas = [||];
}

type 'a store = {
  mutable store_size : int;
  mutable store_data : 'a list;
}

type 'a map = {
  mutable map_size : int;
  mutable map_data : 'a store Tag_map.t;
}

let new_map () = { map_size = 0; map_data = Tag_map.empty }

let add name data map =
  let name = Tag.make name in
  try
    let store = Tag_map.find name map.map_data in
    store.store_size <- store.store_size + 1;
    store.store_data <- data :: store.store_data;
  with Not_found ->
    map.map_size <- map.map_size + 1;
    map.map_data <- Tag_map.add name { store_size = 1; store_data = [data] } map.map_data

let rec rev_fill arr idx l =
  match l with
    | [] ->
        ()
    | x :: l ->
        arr.(idx) <- x;
        rev_fill arr (idx - 1) l

let array_of_store store =
  let arr = Array.make store.store_size (List.hd store.store_data) in
  rev_fill arr (store.store_size - 1) store.store_data;
  arr

let array_of_map map =
  let arr = Array.make map.map_size ("", [||]) in
  let idx = ref 0 in
  Tag_map.iter (fun name store -> arr.(!idx) <- (name, array_of_store store)) map.map_data;
  arr

let parse_xml root str =
  let input = Xmlm.make_input ~strip:true (`String (0, str)) in
  (* Parse the root node. *)
  let rec parse_root () =
    match Xmlm.input input with
      | `El_start ((_, name), _) ->
          if name <> root then raise (Error "invalid XML root");
          (* Ignore the name of the root, just parses its children as
             a list of nodes. *)
          parse_list (new_map ()) (new_map ())
      | `El_end ->
          raise (Error "invalid start of XML")
      | `Data str ->
          raise (Error "unexpected PCDATA at beginning of XML")
      | `Dtd _ ->
          parse_root ()

  (* Parse a sequence of nodes. *)
  and parse_list nodes datas =
    match Xmlm.input input with
      | `El_start ((_, name), _) -> begin
          match Xmlm.peek input with
            | `El_start _ ->
                let node = parse_list (new_map ()) (new_map ()) in
                add name node nodes;
                parse_list nodes datas
            | `El_end ->
                ignore (Xmlm.input input);
                add name empty nodes;
                parse_list nodes datas
            | `Data str -> begin
                ignore (Xmlm.input input);
                match Xmlm.input input with
                  | `El_end ->
                      add name str datas;
                      parse_list nodes datas
                  | _ ->
                      raise (Error "expected end of element after PCDATA in XML")
              end
            | `Dtd _ ->
                raise (Error "unexpected DTD in middle of XML")
        end
      | `El_end ->
          {
            nodes = array_of_map nodes;
            datas = array_of_map datas;
          }
      | `Data str ->
          raise (Error "unexpected PCDATA in middle of node list in XML")
      | `Dtd _ ->
          raise (Error "unexpected DTD in middle of XML")
  in
  try
    parse_root ()
  with Xmlm.Error (pos, error) ->
    raise (Error ("invalid XML: " ^ Xmlm.error_message error))

let bsearch (key : string) arr =
  let rec loop a b =
    if a = b then
      raise Not_found
    else
      let c = (a + b / 2) in
      let key', value = Array.unsafe_get arr c in
      if key == key' then
        value
      else if key < key' then
        loop a c
      else
        loop (c + 1) b
  in
  loop 0 (Array.length arr)

let get_datas name node = bsearch name node.datas
let get_nodes name node = bsearch name node.nodes

let get_data name node =
  match get_datas name node with
    | [|x|] -> x
    | _ -> raise (Error "wrong number of data")

let get_node name node =
  match get_nodes name node with
    | [|x|] -> x
    | _ -> raise (Error "wrong number of node")

(* +-----------------------------------------------------------------+
   | All used tags                                                   |
   +-----------------------------------------------------------------+ *)

let tag_id = Tag.make "id"
let tag_width = Tag.make "width"
let tag_height = Tag.make "height"
let tag_name = Tag.make "name"
let tag_genres = Tag.make "genres"
let tag_years_active = Tag.make "years-active"
let tag_artist = Tag.make "artist"
let tag_artists = Tag.make "artists"
let tag_portrait = Tag.make "portrait"

(* +-----------------------------------------------------------------+
   | Commands                                                        |
   +-----------------------------------------------------------------+ *)

external inflate : string -> string = "mlspot_inflate"

let split_coma str =
  let rec loop i j =
    if j = String.length str then
      [String.sub str i (j - i)]
    else
      match str.[j] with
        | ',' ->
            String.sub str i (j - i) :: loop (j + 1) (j + 1)
        | _ ->
            loop i (j + 1)
  in
  loop 0 0

class portrait node = object
  method id = id_of_string (get_data tag_id node)
  method width = int_of_string (get_data tag_width node)
  method height = int_of_string (get_data tag_height node)
end

class artist node = object
  method name = get_data tag_name node
  method id = id_of_string (get_data tag_id node)
  method portrait = new portrait (get_node tag_portrait node)
  method genres = split_coma (get_data tag_genres node)
  method years_active = List.map int_of_string (split_coma (get_data tag_years_active node))
end

let get_artist session id =
  if id_length id <> 16 then raise (Wrong_id "artist");
  let cache_file = Filename.concat "artists" (string_of_id id ^ ".xml.gz") in
  match_lwt Cache.load session cache_file with
    | Some data ->
        return (new artist (parse_xml tag_artist (inflate data)))
    | None ->
        lwt channel_id, channel_packet, channel_waiter = alloc_channel session#session_parameters in
        try_lwt
          let packet = Packet.create () in
          Packet.add_int16 packet channel_id;
          (* Kind of the browse query: 1 = arstist. *)
          Packet.add_int8 packet 1;
          Packet.add_string packet id;
          Packet.add_int32 packet 0;
          lwt () = send_packet session#session_parameters CMD_BROWSE (Packet.contents packet) in
          lwt data = channel_waiter in
          lwt () = Cache.save session cache_file data in
          return (new artist (parse_xml tag_artist (inflate data)))
        finally
          release_channel session#session_parameters channel_id;
          return ()

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
    return (inflate data)
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
