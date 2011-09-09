(*
 * spotify.ml
 * ----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

open Lwt

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
   | Commands                                                        |
   +-----------------------------------------------------------------+ *)

type command =
  | CMD_SECRETBLK
  | CMD_PING
  | CMD_CHANNELDATA
  | CMD_CHANNELERR
  | CMD_CHANNELABRT
  | CMD_REQKEY
  | CMD_AESKEY
  | CMD_AESKEYERR
  | CMD_SHAHASH
  | CMD_IMAGE
  | CMD_TOKENNOTIFY
  | CMD_COUNTRYCODE
  | CMD_P2P_SETUP
  | CMD_P2P_INITBLK
  | CMD_BROWSE
  | CMD_SEARCH
  | CMD_GETPLAYLIST
  | CMD_CHANGEPLAYLIST
  | CMD_NOTIFY
  | CMD_LOG
  | CMD_PONG
  | CMD_PONGACK
  | CMD_PAUSE
  | CMD_REQUESTAD
  | CMD_REQUESTPLAY
  | CMD_PRODINFO
  | CMD_WELCOME

exception Unknown_command of int

let string_of_command = function
  | CMD_SECRETBLK -> "CMD_SECRETBLK"
  | CMD_PING -> "CMD_PING"
  | CMD_CHANNELDATA -> "CMD_CHANNELDATA"
  | CMD_CHANNELERR -> "CMD_CHANNELERR"
  | CMD_CHANNELABRT -> "CMD_CHANNELABRT"
  | CMD_REQKEY -> "CMD_REQKEY"
  | CMD_AESKEY -> "CMD_AESKEY"
  | CMD_AESKEYERR -> "CMD_AESKEYERR"
  | CMD_SHAHASH -> "CMD_SHAHASH"
  | CMD_IMAGE -> "CMD_IMAGE"
  | CMD_TOKENNOTIFY -> "CMD_TOKENNOTIFY"
  | CMD_COUNTRYCODE -> "CMD_COUNTRYCODE"
  | CMD_P2P_SETUP -> "CMD_P2P_SETUP"
  | CMD_P2P_INITBLK -> "CMD_P2P_INITBLK"
  | CMD_BROWSE -> "CMD_BROWSE"
  | CMD_SEARCH -> "CMD_SEARCH"
  | CMD_GETPLAYLIST -> "CMD_GETPLAYLIST"
  | CMD_CHANGEPLAYLIST -> "CMD_CHANGEPLAYLIST"
  | CMD_NOTIFY -> "CMD_NOTIFY"
  | CMD_LOG -> "CMD_LOG"
  | CMD_PONG -> "CMD_PONG"
  | CMD_PONGACK -> "CMD_PONGACK"
  | CMD_PAUSE -> "CMD_PAUSE"
  | CMD_REQUESTAD -> "CMD_REQUESTAD"
  | CMD_REQUESTPLAY -> "CMD_REQUESTPLAY"
  | CMD_PRODINFO -> "CMD_PRODINFO"
  | CMD_WELCOME -> "CMD_WELCOME"

let command_of_int = function
  | 0x02 -> CMD_SECRETBLK
  | 0x04 -> CMD_PING
  | 0x09 -> CMD_CHANNELDATA
  | 0x0a -> CMD_CHANNELERR
  | 0x0b -> CMD_CHANNELABRT
  | 0x0c -> CMD_REQKEY
  | 0x0d -> CMD_AESKEY
  | 0x0e -> CMD_AESKEYERR
  | 0x10 -> CMD_SHAHASH
  | 0x19 -> CMD_IMAGE
  | 0x1a -> CMD_TOKENNOTIFY
  | 0x1b -> CMD_COUNTRYCODE
  | 0x20 -> CMD_P2P_SETUP
  | 0x21 -> CMD_P2P_INITBLK
  | 0x30 -> CMD_BROWSE
  | 0x31 -> CMD_SEARCH
  | 0x35 -> CMD_GETPLAYLIST
  | 0x36 -> CMD_CHANGEPLAYLIST
  | 0x42 -> CMD_NOTIFY
  | 0x48 -> CMD_LOG
  | 0x49 -> CMD_PONG
  | 0x4a -> CMD_PONGACK
  | 0x4b -> CMD_PAUSE
  | 0x4e -> CMD_REQUESTAD
  | 0x4f -> CMD_REQUESTPLAY
  | 0x50 -> CMD_PRODINFO
  | 0x69 -> CMD_WELCOME
  | cmd -> raise (Unknown_command cmd)

let int_of_command = function
  | CMD_SECRETBLK -> 0x02
  | CMD_PING -> 0x04
  | CMD_CHANNELDATA -> 0x09
  | CMD_CHANNELERR -> 0x0a
  | CMD_CHANNELABRT -> 0x0b
  | CMD_REQKEY -> 0x0c
  | CMD_AESKEY -> 0x0d
  | CMD_AESKEYERR -> 0x0e
  | CMD_SHAHASH -> 0x10
  | CMD_IMAGE -> 0x19
  | CMD_TOKENNOTIFY -> 0x1a
  | CMD_COUNTRYCODE -> 0x1b
  | CMD_P2P_SETUP -> 0x20
  | CMD_P2P_INITBLK -> 0x21
  | CMD_BROWSE -> 0x30
  | CMD_SEARCH -> 0x31
  | CMD_GETPLAYLIST -> 0x35
  | CMD_CHANGEPLAYLIST -> 0x36
  | CMD_NOTIFY -> 0x42
  | CMD_LOG -> 0x48
  | CMD_PONG -> 0x49
  | CMD_PONGACK -> 0x4a
  | CMD_PAUSE -> 0x4b
  | CMD_REQUESTAD -> 0x4e
  | CMD_REQUESTPLAY -> 0x4f
  | CMD_PRODINFO -> 0x50
  | CMD_WELCOME -> 0x69

(* +-----------------------------------------------------------------+
   | Session                                                         |
   +-----------------------------------------------------------------+ *)

exception Closed

type session_parameters = {
  socket : Lwt_unix.file_descr;
  (* The socket used to communicate with the server. *)

  ic : Lwt_io.input_channel;
  oc : Lwt_io.output_channel;

  shn_send : shn_ctx;
  (* The shn context for sending packets. *)

  shn_recv : shn_ctx;
  (* The shn context for receiving packets. *)

  mutable send_iv : int;
  (* Send init vector. *)

  mutable recv_iv : int;
  (* Recv init vector. *)

  mutable next_channel_id : int;
  (* Next ID for channels. *)

  shutdown_waiter : (command * string) Lwt.t;
  shutdown_wakener : (command * string) Lwt.u;
  (* Thread waiting for the session to be closed. *)
}

class session (sp : session_parameters) = object
  val mutable state = Some sp
  method state = state
  method get =
    match state with
      | None -> raise Closed
      | Some sp -> sp
  method close =
    state <- None
end

(* +-----------------------------------------------------------------+
   | Service lookup                                                  |
   +-----------------------------------------------------------------+ *)

external service_lookup_job : unit -> [ `service_lookup ] Lwt_unix.job = "mlspot_service_lookup_job"
external service_lookup_result : [ `service_lookup ] Lwt_unix.job -> (int * string * int) list = "mlspot_service_lookup_result"
external service_lookup_free : [ `service_lookup ] Lwt_unix.job -> unit = "mlspot_service_lookup_free" "noalloc"

let service_lookup () =
  Lwt_unix.execute_job (service_lookup_job ()) service_lookup_result service_lookup_free

(* +-----------------------------------------------------------------+
   | Packets                                                         |
   +-----------------------------------------------------------------+ *)

module Packet = struct

  type t = {
    mutable buf : string;
    mutable ofs : int;
  }

  let create () = {
    buf = String.create 256;
    ofs = 0;
  }

  let length packet = packet.ofs

  let contents packet = String.sub packet.buf 0 packet.ofs

  let reset packet = packet.ofs <- 0

  let extend packet size =
    if packet.ofs + size > String.length packet.buf then begin
      let buf = String.create (String.length packet.buf * 2) in
      String.unsafe_blit packet.buf 0 buf 0 packet.ofs;
      packet.buf <- buf
    end

  let add_string packet str =
    let len = String.length str in
    extend packet len;
    String.unsafe_blit str 0 packet.buf packet.ofs len;
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

let send_packet sp command packet =
  let nonce = String.create 4 in
  let iv = sp.send_iv in
  sp.send_iv <- iv + 1;
  String.unsafe_set nonce 0 (Char.unsafe_chr (iv lsr 24));
  String.unsafe_set nonce 1 (Char.unsafe_chr (iv lsr 16));
  String.unsafe_set nonce 2 (Char.unsafe_chr (iv lsr 8));
  String.unsafe_set nonce 3 (Char.unsafe_chr iv);
  shn_nonce sp.shn_send nonce 0 4;
  let len = Packet.length packet in
  let buffer = String.create (3 + len + 4) in
  String.unsafe_set buffer 0 (Char.unsafe_chr (int_of_command command));
  String.unsafe_set buffer 1 (Char.unsafe_chr (len lsr 8));
  String.unsafe_set buffer 2 (Char.unsafe_chr len);
  String.blit packet.Packet.buf 0 buffer 3 packet.Packet.ofs;
  shn_encrypt sp.shn_send buffer 0 (3 + len);
  shn_finish sp.shn_send buffer (3 + len) 4;
  Lwt_io.write_from_exactly sp.oc buffer 0 (3 + len + 4)

let recv_packet sp =
  let nonce = String.create 4 in
  let iv = sp.recv_iv in
  sp.recv_iv <- iv + 1;
  String.unsafe_set nonce 0 (Char.unsafe_chr (iv lsr 24));
  String.unsafe_set nonce 1 (Char.unsafe_chr (iv lsr 16));
  String.unsafe_set nonce 2 (Char.unsafe_chr (iv lsr 8));
  String.unsafe_set nonce 3 (Char.unsafe_chr iv);
  shn_nonce sp.shn_recv nonce 0 4;
  let header = String.create 3 in
  lwt () = Lwt_io.read_into_exactly sp.ic header 0 3 in
  shn_decrypt sp.shn_recv header 0 3;
  let len = (Char.code (String.unsafe_get header 1) lsl 8) lor Char.code (String.unsafe_get header 2) in
  let packet_len = len + 4 in
  let payload = String.create packet_len in
  lwt () = Lwt_io.read_into_exactly sp.ic payload 0 packet_len in
  shn_decrypt sp.shn_recv payload 0 packet_len;
  return (command_of_int (Char.code (String.unsafe_get header 0)), payload)

(* +-----------------------------------------------------------------+
   | Dispatching                                                     |
   +-----------------------------------------------------------------+ *)

let rec loop_dispatch sp =
  lwt () =
    try_lwt
      lwt command, payload = pick [recv_packet sp; sp.shutdown_waiter] in
      lwt () = Lwt_io.printlf "command %s received:" (string_of_command command) in
      lwt () = Lwt_io.hexdump Lwt_io.stdout payload in
      return ()
    with
      | Unknown_command command ->
          ignore (Lwt_log.warning_f "unknown command received (0x%02x)" command);
          return ()
      | Closed ->
          return ()
      | exn ->
          exit 1
  in
  loop_dispatch sp

(* +-----------------------------------------------------------------+
   | Connection                                                      |
   +-----------------------------------------------------------------+ *)

exception Connection_failure of string

let dh_parameters = {
  Cryptokit.DH.p = "\xff\xff\xff\xff\xff\xff\xff\xff\xc9\x0f\xda\xa2\x21\x68\xc2\x34\xc4\xc6\x62\x8b\x80\xdc\x1c\xd1\x29\x02\x4e\x08\x8a\x67\xcc\x74\x02\x0b\xbe\xa6\x3b\x13\x9b\x22\x51\x4a\x08\x79\x8e\x34\x04\xdd\xef\x95\x19\xb3\xcd\x3a\x43\x1b\x30\x2b\x0a\x6d\xf2\x5f\x14\x37\x4f\xe1\x35\x6d\x6d\x51\xc2\x45\xe4\x85\xb5\x76\x62\x5e\x7e\xc6\xf4\x4c\x42\xe9\xa6\x3a\x36\x20\xff\xff\xff\xff\xff\xff\xff\xff";
  Cryptokit.DH.g = "\x02";
  Cryptokit.DH.privlen = 160;
}

let connect ~username ~password =
  (* Service lookup. *)
  lwt servers =
    try_lwt
      service_lookup ()
    with Unix.Unix_error (err, _, _) ->
      ignore (Lwt_log.warning_f "service lookup failed: %s" (Unix.error_message err));
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
          raise_lwt (Connection_failure "cannot connect to any server")
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

  lwt socket = loop address_infos in

  try_lwt
    let ic = Lwt_io.make ~mode:Lwt_io.input (Lwt_bytes.read socket)
    and oc = Lwt_io.make ~mode:Lwt_io.output (Lwt_bytes.write socket) in

    let rng = Cryptokit.Random.pseudo_rng "lkjsdflksjizejijqklsjflksqjflksnqk,nklndqlk,klqnfd" in

    (* Generate random data. *)
    let client_random = Cryptokit.Random.string rng 16 in

    (* Generate a secret. *)
    let secret = Cryptokit.DH.private_secret ~rng dh_parameters in

    (* Generate a new RSA key. *)
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
        | '\x01' -> raise (Connection_failure "client upgrade recquired")
        | '\x03' -> raise (Connection_failure "user not found")
        | '\x04' -> raise (Connection_failure "account has been disabled")
        | '\x06' -> raise (Connection_failure "you need to complete your account details")
        | '\x09' -> raise (Connection_failure "country mismatch")
        | _ -> raise (Connection_failure "unknown error")
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
      raise (Connection_failure "unexpected puzzle challenge");
    let denominator = 1 lsl (Char.code puzzle.[1]) - 1 in
    let magic =
      (Char.code puzzle.[2] lsl 24)
      lor (Char.code puzzle.[3] lsl 16)
      lor (Char.code puzzle.[4] lsl 8)
      lor (Char.code puzzle.[5])
    in

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
      let nominator =
        (Char.code digest.[16] lsl 24)
        lor (Char.code digest.[17] lsl 16)
        lor (Char.code digest.[18] lsl 8)
        lor (Char.code digest.[19])
      in
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
    ctx#add_substring packet.Packet.buf 0 packet.Packet.ofs;
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
    if status <> 0 then raise (Connection_failure "authentication failed");

    (* Read the payload length. *)
    lwt payload_length = Lwt_io.read_char ic >|= Char.code in

    (* Read the payload. *)
    let payload = String.create payload_length in
    lwt () = Lwt_io.read_into_exactly ic payload 0 payload_length in

    let shutdown_waiter, shutdown_wakener = wait () in
    let sp = {
      socket;
      ic;
      oc;
      shn_send;
      shn_recv;
      send_iv = 0;
      recv_iv = 0;
      next_channel_id = 0;
      shutdown_waiter;
      shutdown_wakener;
    } in
    ignore (loop_dispatch sp);
    return (new session sp)
  with exn ->
    Lwt_unix.shutdown socket Unix.SHUTDOWN_ALL;
    lwt () = Lwt_unix.close socket in
    raise_lwt exn

let close session =
  match session#state with
    | None ->
        return ()
    | Some sp ->
        session#close;
        wakeup_exn sp.shutdown_wakener Closed;
        lwt () = Lwt_io.flush sp.oc in
        Lwt_unix.shutdown sp.socket Unix.SHUTDOWN_ALL;
        Lwt_unix.close sp.socket

(* +-----------------------------------------------------------------+
   | IDs                                                             |
   +-----------------------------------------------------------------+ *)

type id = string

exception Id_parse_failure
exception Invalid_id_length

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
   | Commands                                                        |
   +-----------------------------------------------------------------+ *)

let get_artist session id =
  assert false
