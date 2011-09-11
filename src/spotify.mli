(*
 * spotify.mli
 * -----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

(** Spotify library *)

val xdg_cache_home : string
  (** The path where the user cache data should be stored, according
      the XDG specification. *)

(** {6 General errors} *)

exception Offline
  (** Exception raised when trying to do something which require being
      online on an offline session. *)

exception Logged_out
  (** Exception raised by pending operation when the session is logged
      out in another thread. *)

exception Disconnected
  (** Exception raised when the connection to a spotify server is
      lost. In this case you need to logout using {!logout} and
      relogin if you want to. *)

exception Error of string
  (** An error occured while receiving data. *)

(** {6 Sessions} *)

type session
  (** Type of sessions. *)

val create : ?use_cache : bool -> ?cache_dir : string -> unit -> session
  (** [connect ?use_cache ?cache_dir ()] create a new spotify session.

      If [use_cache] is [true] (the default) then data recevied from
      spotify will be stored in [cache_dir], which default to
      [Filename.concat xdg_cache_home "mlspot"].

      The returned session is in offline mode. *)

exception Connection_failure of string
  (** Exception raised by {!loggin} when the connection to the spotify
      server fail. *)

exception Authentication_failure of string
  (** Exception raised by {!loggin} when the authentication fail. *)

val login : session -> username : string -> password : string -> unit Lwt.t
  (** [login ?rsa session ~username ~password] logs in to the spotify
      service using the given credentials. If the user is already
      logged, then it is logged out and relogged in.

      If the connection or authentication fail, the session stay in
      offline mode.

      @raise Connection_failure if it cannot reach the spotify server
      @raise Authentication_failure if the authentication fails. *)

val logout : session -> unit Lwt.t
  (** [logout session] logs out from the spotify service. Does nothing
      if the session is in offline mode. *)

val online : session -> bool
  (** Returns whether the user is currently connected. *)

val get_use_cache : session -> bool
  (** Returns whether the given session use the local cache. *)

val set_use_cache : session -> bool -> unit
  (** Set whether to use the local cache in the given session. *)

val get_cache_dir : session -> string
  (** Return the cache directory used in the given session. *)

val set_cache_dir : session -> string -> unit
  (** Set the cache directory used in the given session. *)

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

(** {6 Enumerations} *)

exception Out_of_bounds
  (** Exception raised when trying to access an element which is
      outside the range of an enumeration. *)

(** Type of enumeration of fixed length holding elements of type
    ['a]. The first argument is an array of elements and the second is
    a function which maps elemnt of the array. *)
class ['a] enum : 'b array -> ('b -> 'a) -> object
  method length : int
    (** Length of the enumeration. *)

  method get : int -> 'a
    (** Returns the nth element of the enumeration.

        @raise Out_of_bounds if the index is invalid. *)

  method to_list : 'a list
    (** Converts the enumeration to a list. *)

  method to_array : 'a array
    (** Concerts the enumeration to an array. *)

  method iter : ('a -> unit) -> unit
    (** [iter f] applies [f] on each elements of the enumeration. *)

  method fold : 'b. ('a -> 'b -> 'b) -> 'b -> 'b
    (** [fold f acc] applies [f] on each elements of the enumeration,
        accumulating a result. *)
end

(** {6 Types} *)

class type portrait = object
  method id : id
  method width : int
  method height : int
end

class type bio = object
  method text : string
  method portraits : portrait enum
end

class type restriction = object
  method catalogues : string list
  method forbidden : string list
  method allowed : string list
end

class type similar_artist = object
  method name : string
  method id : id
  method portrait : id
  method genres : string list
  method years_active : int list
  method restrictions : restriction enum
end

class type file = object
  method id : id
  method format : string
  method bitrate : int
end

class type alternative = object
  method id : id
  method files : file enum
  method restrictions : restriction enum
end

class type track = object
  method id : id
  method title : string
  method explicit : bool
  method artist : string
  method artist_id : id
  method track_number : int
  method length : float
  method files : file enum
  method popularity : float
  method external_ids : (string * string) enum
  method alternatives : alternative enum
end

class type disc = object
  method disc_number : int
  method name : string option
  method tracks : track enum
end

class type album = object
  method name : string
  method id : id
  method artist : string
  method artist_id : id
  method album_type : string
  method year : int
  method cover : id
  method copyrights : string enum
  method restrictions : restriction enum
  method external_ids : (string * string) enum
  method discs : disc enum
end

class type artist = object
  method name : string
  method id : id
  method portrait : portrait
  method bios : bio enum
  method similar_artists : similar_artist enum
  method genres : string list
  method years_active : int list
  method albums : album enum
end

(** {6 Commands} *)

val get_artist : session -> id -> artist Lwt.t
  (** [get_artist session id] returns the artist whose ID is
      [id]. [id] must be of length 16. *)

(** {6 Search} *)

(*class search_result = object
  method did_you_mean : string option
    (** Suggestion. *)

  method total_artists : int
    (** The total number of artist that match the query. *)

  method total_albums : int
    (** The total number of albums that match the query. *)

  method total_tracks : int
    (** The total number of tracks that match the query. *)

  method artists : artist_search enum
  method albums : album_search enum
  method tracks : track enum
end
*)
val search : session -> ?offset : int -> ?length : int -> string -> string Lwt.t
