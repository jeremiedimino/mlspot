(*
 * search.ml
 * ---------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

open Lwt

lwt () =
  if Array.length Sys.argv <> 4 then begin
    prerr_endline "Usage: artistinfo <username> <password> <query>";
    exit 2
  end;

  (* Create a new spotify session. *)
  let session = Spotify.create () in

  (* Connect to spotify. *)
  lwt () = Spotify.login session ~username:Sys.argv.(1) ~password:Sys.argv.(2) in

  (* Perform the search. *)
  lwt search = Spotify.search session Sys.argv.(3) in

  (* Display the result. *)
  lwt () =
    Lwt_io.printf "\
total artists: %d
total albums: %d
total tracks: %d
"
      search#total_artists search#total_albums search#total_tracks
  in

  lwt () = Lwt_io.printf "\nartist results:\n" in
  lwt () =
    for_lwt i = 0 to search#artists#length - 1 do
      Lwt_io.printlf "  %s" (search#artists#get i)#name
    done
  in

  lwt () = Lwt_io.printf "\nalbum results:\n" in
  lwt () =
    for_lwt i = 0 to search#albums#length - 1 do
      Lwt_io.printlf "  %s" (search#albums#get i)#name
    done
  in

  lwt () = Lwt_io.printf "\ntrack results:\n" in
  lwt () =
    for_lwt i = 0 to search#tracks#length - 1 do
      Lwt_io.printlf "  %s" (search#tracks#get i)#title
    done
  in

  return ()
