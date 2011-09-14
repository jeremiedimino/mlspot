(*
 * spotify_printers.mli
 * --------------------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

(** Printers for the toplevel. *)

open Format
open Spotify

val print_spotify : formatter -> < link : Spotify.link; .. > -> unit
