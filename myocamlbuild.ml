(*
 * myocamlbuild.ml
 * ---------------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

(* OASIS_START *)
(* OASIS_STOP *)

open Ocamlbuild_plugin

let () =
  dispatch
    (fun hook ->
       dispatch_default hook;
       match hook with
         | Before_options ->
             Options.make_links := false

         | After_rules ->
             (* Add the lwt package for C files. *)
             flag ["c"; "compile"; "use_lwt_headers"] & S[A"-package"; A"lwt"];

             (* Internal syntax extension *)
             flag ["ocaml"; "compile"; "pa_xml"] & S[A"-ppopt"; A "syntax/pa_xml.cmo"];
             flag ["ocaml"; "ocamldep"; "pa_xml"] & S[A"-ppopt"; A "syntax/pa_xml.cmo"];
             flag ["ocaml"; "doc"; "pa_xml"] & S[A"-ppopt"; A "syntax/pa_xml.cmo"];
             dep ["ocaml"; "ocamldep"; "pa_xml"] ["syntax/pa_xml.cmo"]

         | _ ->
             ())
