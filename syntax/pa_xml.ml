(*
 * pa_xml.ml
 * ---------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

(* Syntax extension for effecient XML parsing. *)

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

let map_field tag = object
  inherit Ast.map as super

  method expr e =
    match super#expr e with
      | <:expr@_loc< DATA >> -> <:expr< get_data $lid:tag$ node >>
      | <:expr@_loc< NODE >> -> <:expr< get_node $lid:tag$ node >>
      | <:expr@_loc< DATAS >> -> <:expr< get_datas $lid:tag$ node >>
      | <:expr@_loc< NODES >> -> <:expr< get_nodes $lid:tag$ node >>
      | e -> e
end

let sort tags =
  let module String_set = Set.Make (String) in
  String_set.elements (List.fold_left (fun map tag -> String_set.add tag map) String_set.empty tags)

EXTEND Gram
  GLOBAL: str_item;

  field:
    [ [ name = STRING; "="; e = expr LEVEL "top" -> (name, (map_field ("tag_" ^ id_of_tag name))#expr e) ] ];

  fields:
    [ [ field = field; ";"; fields = fields -> field :: fields
      | field = field -> [field]
      | -> [] ] ];

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

      | "XML"; id = LIDENT; "="; "{"; fields = fields; "}" ->
          let fields =
            Ast.crSem_of_list
              (List.map
                 (fun (name, e) ->
                    let id = id_of_tag name in
                    <:class_str_item<
                      val $lid:id$ = lazy $e$
                      method $lid:id$ = Lazy.force $lid:id$
                    >>)
                 fields)
          in
          <:str_item<
            class $lid:id$ node = object
              $fields$
            end
          >>
      ] ];
END

