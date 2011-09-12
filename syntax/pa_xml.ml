(*
 * pa_xml.ml
 * ---------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

(* Create a sorted array of tags with constants. *)

open Camlp4
open Camlp4.PreCast
open Syntax

(* Convert an XML tag to an ocaml idenfitier. *)
let id_of_tag tag =
  let id = String.create (String.length tag) in
  for i = 0 to String.length tag - 1 do
    match tag.[i] with
      | 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' as ch ->
          id.[i] <- ch
      | _ ->
          id.[i] <- '_'
  done;
  id

let rec mapi f i l =
  match l with
    | [] ->
        []
    | x :: l ->
        f i x :: mapi f (i + 1) l

let sort tags =
  let module String_set = Set.Make (String) in
  String_set.elements (List.fold_left (fun map tag -> String_set.add tag map) String_set.empty tags)

EXTEND Gram
  GLOBAL: str_item;

  tags:
    [ [ tag = STRING; ";"; tags = tags -> tag :: tags
      | tag = STRING -> [tag]
      | -> [] ] ];

  str_item:
    [ [ "TAGS"; "["; tags = tags; "]" ->
          let tags = sort tags in
          let arr_expr = Ast.ExArr (_loc, Ast.exSem_of_list (List.map (fun tag -> Ast.ExStr (_loc, tag)) tags)) in
          let tags_def = Ast.stSem_of_list (mapi (fun idx tag -> <:str_item< let $lid:"tag_" ^ id_of_tag tag$ = $int:string_of_int idx$ >>) 0 tags) in
          <:str_item<
            let tags = $arr_expr$;;
            $tags_def$;;
          >>
      ] ];
END

