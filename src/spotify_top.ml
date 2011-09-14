(*
 * spotify_top.ml
 * --------------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 *)

let printers = [
  "Spotify_printers.print_spotify";
]

let install name =
  let lexbuf = Lexing.from_string ("#install_printer " ^ name ^ ";;") in
  let phrase = !Toploop.parse_toplevel_phrase lexbuf in
  Toploop.execute_phrase false Format.err_formatter phrase

let () =
  List.iter
    (fun printer ->
       if not (install printer) then
         Printf.eprintf "failed to install the printer %S\n%!" printer)
    printers
