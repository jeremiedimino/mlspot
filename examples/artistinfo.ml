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

  (* Connect to spotify. *)
  lwt session = Spotify.connect ~username:Sys.argv.(1) ~password:Sys.argv.(2) in

  (* Get artist informations. *)
  lwt str = Spotify.get_artist session (Spotify.id_of_string Sys.argv.(3)) in

  lwt () = Lwt_io.print str in

  return ()
