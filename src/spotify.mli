(*
 * spotify.mli
 * -----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

(** Spotify library *)

type session
  (** Type of sessions. *)

exception Connection_failure of string
  (** Exception raised when the connection failed. *)

val connect : username : string -> password : string -> session Lwt.t
  (** [connect ~username ~password] connects to spotify using the
      given credentials. *)
