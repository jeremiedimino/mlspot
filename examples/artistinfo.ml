(*
 * artistinfo.ml
 * -------------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

open Lwt

lwt () =
  if Array.length Sys.argv <> 4 then begin
    prerr_endline "Usage: artistinfo <username> <password> <artist-id>";
    exit 2
  end;

  let session = Spotify.create () in

  (* Connect to spotify. *)
  lwt () = Spotify.login session ~username:Sys.argv.(1) ~password:Sys.argv.(2) in

(*  lwt search = Spotify.search session "tina aren" in*)

  (* Get artist informations. *)
  lwt artist = Spotify.get_artist session (Spotify.id_of_string Sys.argv.(3)) in

  return ()
