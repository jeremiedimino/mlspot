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

exception Closed
  (** Exception raised when trying to use a closed session. *)

(** {6 Connection} *)

exception Connection_failure of string
  (** Exception raised when the connection failed. *)

val connect : username : string -> password : string -> session Lwt.t
  (** [connect ~username ~password] connects to spotify using the
      given credentials. *)

val close : session -> unit Lwt.t
  (** Close the connection to a spotify server. *)

(** {6 IDs} *)

type id
  (** Type of ids. *)

exception Id_parse_failure
  (** Exception when trying to parse an invalid ID. *)

exception Wrong_id of string
  (** Exception raised when using an ID for a task which require an ID
      of a different length. The argument is the kind of ID
      expected. For example artist IDs are of length 16, if you try to
      use {!get_artist} with an ID of length different than 16, then
      [Wrong_id "artist"] will be raised. *)

val id_length : id -> int
  (** Returns the length of the given ID. *)

val id_of_string : string -> id
  (** Convert a string to an ID. The string must be hexencoded and of
      even length. It raises {!Id_parse_failure} on errors. *)

val string_of_id : id -> string
  (** Return the string representation of an ID. *)

(** {6 Errors} *)

exception Error of string
  (** An error occured while receiving data. *)

(** {6 Artists} *)

class type portrait = object
  method id : id
  method width : int
  method height : int
end

class type artist = object
  method name : string
  method id : id
  method portrait : portrait
  method genres : string list
  method years_active : int list
end

val get_artist : session -> id -> artist Lwt.t
  (** [get_artist session id] returns the artist whose ID is
      [id]. [id] must be of length 16. *)

val search : session -> ?offset : int -> ?length : int -> string -> string Lwt.t
