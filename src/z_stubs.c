/*
 * z_stubs.c
 * ---------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/fail.h>
#include <caml/alloc.h>
#include <caml/callback.h>

#include <string.h>
#include <stdio.h>
#include <zlib.h>

CAMLprim value mlspot_inflate(value string)
{
  struct z_stream_s z;
  memset(&z, 0, sizeof z);

  if (inflateInit2(&z, -MAX_WBITS) != Z_OK)
    caml_raise_with_arg(*caml_named_value("mlspot:error"), caml_copy_string("cannot create zlib stream"));

  size_t len = caml_string_length(string) * 2;
  unsigned char *buf = malloc(len);
  if (buf == NULL) {
    perror("cannot allocate memory");
    abort();
  }

  z.next_in = (unsigned char *)String_val(string) + 10;
  z.avail_in = caml_string_length(string) - 10;

  int loop = 1;
  int ofs = 0;
  while (loop) {
    z.avail_out = len - ofs;
    z.next_out = buf + ofs;

    switch (inflate(&z, Z_NO_FLUSH)) {
    case Z_OK:
      ofs = len - z.avail_out;
      if (z.avail_out == 0) {
        len = len * 2;
        buf = realloc(buf, len);
        if (buf == NULL) {
          perror("cannot allocate memory");
          abort();
        }
      }
      break;

    case Z_STREAM_END:
      loop = 0;
      break;

    default:
      free(buf);
      caml_raise_with_arg(*caml_named_value("mlspot:error"), caml_copy_string("inflate error"));
    }
  }

  value res = caml_alloc_string(len - z.avail_out);
  memcpy(String_val(res), buf, len - z.avail_out);
  return res;
}
