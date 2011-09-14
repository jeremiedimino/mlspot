(*
 * spotify_printers.ml
 * -------------------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

open Format
open Spotify

let print_spotify pp obj =
  Format.pp_print_string pp "<";
  Format.pp_print_string pp (Spotify.uri_of_link obj#link);
  Format.pp_print_string pp ">"
