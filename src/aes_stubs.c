/*
 * aes_stubs.c
 * -----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 */

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/fail.h>

#include <string.h>

#include "aes.h"

CAMLprim value mlspot_aes_rijndael_key_setup_enc(value key)
{
  u32 rk[4 * (MAXNR + 1)];
  int nr = rijndaelKeySetupEnc(rk, (const u8*)String_val(key), caml_string_length(key) * 8);
  if (nr == 0) caml_invalid_argument("rijndael_key_setup_enc");
  int len = 4 * (nr + 1) * sizeof(u32);
  value result = caml_alloc_string(len);
  memcpy(String_val(result), rk, len);
  return result;
}

CAMLprim value mlspot_aes_rijndael_key_setup_dec(value key)
{
  u32 rk[4 * (MAXNR + 1)];
  int nr = rijndaelKeySetupEnc(rk, (const u8*)String_val(key), caml_string_length(key) * 8);
  if (nr == 0) caml_invalid_argument("rijndael_key_setup_dec");
  int len = 4 * (nr + 1) * sizeof(u32);
  value result = caml_alloc_string(len);
  memcpy(String_val(result), rk, len);
  return result;
}

CAMLprim value mlspot_aes_rijndael_encrypt(value rk, value src, value dst)
{
  if (caml_string_length(src) != 16 || caml_string_length(dst) != 16)
    caml_invalid_argument("rijndael_encrypt");
  rijndaelEncrypt((u32*)String_val(rk), caml_string_length(rk) / (4 * sizeof(u32)) - 1, (u8*)String_val(src), (u8*)String_val(dst));
  return Val_unit;
}

CAMLprim value mlspot_aes_rijndael_decrypt(value rk, value src, value dst)
{
  if (caml_string_length(src) != 16 || caml_string_length(dst) != 16)
    caml_invalid_argument("rijndael_encrypt");
  rijndaelEncrypt((u32*)String_val(rk), caml_string_length(rk) / (4 * sizeof(u32)) - 1, (u8*)String_val(src), (u8*)String_val(dst));
  return Val_unit;
}
