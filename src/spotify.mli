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
  (** Type of ids. Note that ids are comparable using [==]. *)

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

val id_hash : id -> int
  (** Returns the hash of the given id. *)

val id_of_string : string -> id
  (** Convert a string to an ID. The string must be hexencoded and of
      even length. It raises {!Id_parse_failure} on errors. *)

val string_of_id : id -> string
  (** Return the string representation of an ID. *)

(** {6 Types} *)

class type portrait = object
  method id : id
  method width : int
  method height : int
end

class type biography = object
  method text : string
  method portraits : portrait list
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
  method restrictions : restriction list
end

class type file = object
  method id : id
  method format : string
  method bitrate : int
end

class type alternative = object
  method id : id
  method files : file list
  method restrictions : restriction list
end

class type track = object
  method id : id
  method title : string
  method explicit : bool
  method artists : string list
  method artists_id : id list
  method album : string
  method album_id : id
  method album_artist : string
  method album_artist_id : id
  method year : int
  method number : int
  method length : float
  method files : file list
  method cover : id
  method popularity : float
  method external_ids : (string * string) list
  method alternatives : alternative list
end

class type disc = object
  method number : int
  method name : string option
  method tracks : track list
end

class type album = object
  method name : string
  method id : id
  method artist : string
  method artist_id : id
  method album_type : string
  method year : int
  method cover : id
  method copyrights : (string * string) list
  method restrictions : restriction list
  method external_ids : (string * string) list
  method discs : disc list
end

class type artist = object
  method name : string
  method id : id
  method portrait : portrait
  method biographies : biography list
  method similar_artists : similar_artist list
  method genres : string list
  method years_active : int list
  method albums : album list
end

class type artist_search = object
  method id : id
  method name : string
  method portrait : portrait option
  method popularity : float
  method restrictions : restriction list
end

class type album_search = object
  method id : id
  method name : string
  method artist : string
  method artist_id : id
  method cover : id
  method popularity : float
  method restrictions : restriction list
  method external_ids : (string * string) list
end

class type search_result = object
  method did_you_mean : string option
    (** Suggestion. *)

  method total_artists : int
    (** The total number of artist that match the query. *)

  method total_albums : int
    (** The total number of albums that match the query. *)

  method total_tracks : int
    (** The total number of tracks that match the query. *)

  method artists : artist_search list
  method albums : album_search list
  method tracks : track list
end

(** {6 Commands} *)

val get_artist : session -> id -> artist Lwt.t
  (** [get_artist session id] returns the artist whose ID is
      [id]. [id] must be of length 16. *)

val get_album : session -> id -> album Lwt.t
  (** [get_album session id] returns the album whose ID is [id]. [id]
      must be of length 16. *)

val get_track : session -> id -> track Lwt.t
  (** [get_track session id] returns the track whose ID is [id]. [id]
      must be of length 16. *)

val get_tracks : session -> id list -> track list Lwt.t
  (** [get_track session ids] returns the tracks whose ID are
      [ids]. [ids] must all be of length 16. *)

val get_image : session -> id -> string Lwt.t
  (** [get_image session id] returns the image whose ID is [id]. [id]
      must be of length 20. *)

val search : session -> ?offset : int -> ?length : int -> string -> search_result Lwt.t
  (** [search session ?offset ?length query] performs the given
      search. [offset] represent the offset the first response to get
      in the list of all response. It default to [0]. [length] is the
      maximum number of responses to return. It default to [1000]. *)

