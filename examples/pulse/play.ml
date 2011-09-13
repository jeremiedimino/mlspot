(*
 * play.ml
 * -------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

open Lwt

lwt () =
  if Array.length Sys.argv <> 4 then begin
    prerr_endline "Usage: artistinfo <username> <password> <track-uri>";
    exit 2
  end;

  let id =
    match Spotify.link_of_uri Sys.argv.(3) with
      | Spotify.Track id ->
          id
      | _ ->
          prerr_endline "not a track uri";
          exit 2
  in

  (* Create a new spotify session. *)
  let session = Spotify.create () in

  (* Connect to spotify. *)
  lwt () = Spotify.login session ~username:Sys.argv.(1) ~password:Sys.argv.(2) in

  (* Get the track. *)
  lwt track = Spotify.get_track session id in

  (* Get the id of the file with the highest bitrate. *)
  let file_id =
    match track#files with
      | [] ->
          prerr_endline "no file for this track";
          exit 2
      | files ->
          (List.hd (List.sort (fun file1 file2 -> file2#bitrate - file1#bitrate) files))#id
  in

  (* Open the stream. *)
  lwt stream = Spotify.open_track session ~track_id:track#id ~file_id in

  (* Open a pulseaudio connection. *)
  let sample = {
    Pulseaudio.sample_chans = Spotify.channels stream;
    Pulseaudio.sample_rate = Spotify.sample_rate stream;
    Pulseaudio.sample_format = Pulseaudio.Sample_format_s16le;
  } in
  let pulse = Pulseaudio.Simple.create ~client_name:"play" ~dir:Pulseaudio.Dir_playback ~stream_name:track#title ~sample () in

  (* Play it. *)
  let buffer = Array.make_matrix (Spotify.channels stream) 1024 0. in
  try_lwt
    while_lwt true do
      lwt n = Spotify.read stream buffer 0 1024 in
      Lwt_preemptive.detach (fun () -> Pulseaudio.Simple.write pulse buffer 0 n) ()
    done
  with End_of_file ->
    return ()
