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

  lwt stream = Spotify.fetch session ~track_id:(Spotify.id_of_string "41a8bfa7aff94d21bc1c24dba6375330") ~file_id:(Spotify.id_of_string "4fac45c10b552dc02add04f474f5a6e5f35e0cdd") ~block_size:(1 lsl 20) () in
  lwt () = Lwt_stream.iter (fun _ -> print_endline "toto") stream in

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
    Lwt_list.iter_s
      (fun artist -> Lwt_io.printlf "  %s" artist#name)
      search#artists
  in

  lwt () = Lwt_io.printf "\nalbum results:\n" in
  lwt () =
    Lwt_list.iter_s
      (fun album -> Lwt_io.printlf "  %s" album#name)
      search#albums
  in

  lwt () = Lwt_io.printf "\ntrack results:\n" in
  lwt () =
    Lwt_list.iter_s
      (fun track -> Lwt_io.printlf "  %s" track#title)
      search#tracks
  in

  return ()
