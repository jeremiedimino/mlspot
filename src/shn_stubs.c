/*
 * shn_stubs.c
 * -----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 */

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <string.h>
#include "shn.h"

static struct custom_operations shn_ctx_ops = {
  "spotify.shn",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default
};

#define Shn_ctx_val(value) (shn_ctx*)Data_custom_val(value)

CAMLprim value mlspot_shn_ctx_new()
{
  value result = caml_alloc_custom(&shn_ctx_ops, sizeof (shn_ctx), 0, 1);
  memset(Shn_ctx_val(result), 0, sizeof (shn_ctx));
  return result;
}

#define STUB(name)                                                      \
  CAMLprim value mlspot_shn_##name(value ctx, value str, value ofs, value len) \
  {                                                                     \
    shn_##name(Shn_ctx_val(ctx), (UCHAR*)String_val(str) + Int_val(ofs), Int_val(len)); \
    return Val_unit;                                                    \
  }

STUB(key)
STUB(nonce)
STUB(stream)
STUB(maconly)
STUB(encrypt)
STUB(decrypt)
STUB(finish)
