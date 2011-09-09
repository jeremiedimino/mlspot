/*
 * dns_stubs.c
 * -----------
 * Copyright : (c) 2011, Jeremie Dimino <jeremie@dimino.org>
 * Licence   : BSD3
 *
 * This file is a part of mlspot.
 */

/* DNS utilities */

#include <lwt_unix.h>
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/unixsupport.h>

#define NAME "_spotify-client._tcp.spotify.com"

#if defined(LWT_ON_WINDOWS)

/* +-----------------------------------------------------------------+
   | Windows stubs                                                   |
   +-----------------------------------------------------------------+ */

#else

/* +-----------------------------------------------------------------+
   | Unix stubs                                                      |
   +-----------------------------------------------------------------+ */

#include <netinet/in.h>
#include <arpa/nameser.h>
#include <resolv.h>
#include <errno.h>

/* Structure used for the Lwt unix job. */
struct job_service_lookup {
  /* Lwt stuff. */
  struct lwt_unix_job job;

  /* The answer. */
  unsigned char answer[65536];

  /* The length of the answer. */
  int answer_length;

  /* errno. */
  int error_code;
};

/* Cast a value into a service list job. */
#define Job_service_lookup_val(v) *(struct job_service_lookup**)Data_custom_val(v)

/* Perform the DNS query. */
static void worker_service_lookup(struct job_service_lookup *job)
{
  job->answer_length = res_search(NAME, ns_c_in, ns_t_srv, job->answer, 65536);
  job->error_code = errno;
}

/* Create the job for service listing. */
CAMLprim value mlspot_service_lookup_job()
{
  struct job_service_lookup *job = lwt_unix_new(struct job_service_lookup);
  job->job.worker = (lwt_unix_job_worker)worker_service_lookup;
  return lwt_unix_alloc_job(&(job->job));
}

/* Read the result. */
CAMLprim value mlspot_service_lookup_result(value val_job)
{
  CAMLparam1(val_job);
  CAMLlocal3(result, node, record);

  struct job_service_lookup *job = Job_service_lookup_val(val_job);

  /* Fail if res_search failed. */
  if (job->answer_length < 0) unix_error(job->error_code, "res_search", Nothing);

  HEADER *header = (HEADER*)job->answer;
  int qdcount = ntohs(header->qdcount);
  int ancount = ntohs(header->ancount);

  /* Skip the header. */
  unsigned char *ptr = job->answer + NS_HFIXEDSZ;

  /* Skip the questions. */
  while (qdcount--) {
    int name_len = dn_skipname (ptr, job->answer + job->answer_length);
    if (name_len < 0) uerror("dn_skipname", Nothing);
    ptr = ptr + name_len + NS_QFIXEDSZ;
  }

  /* Parse the answers. */
  result = Val_int(0);
  while (ancount--) {
    /* Skip the host name. */
    int name_len = dn_skipname(ptr, job->answer + job->answer_length);
    if (name_len < 0) uerror("dn_skipname", Nothing);

    ptr += name_len;

    /* Read the record type. */
    int type;
    GETSHORT(type, ptr);

    /* Skip the class and TTL. */
    ptr += 6;

    /* Read the length of data. */
    int data_len;
    GETSHORT(data_len, ptr);

    /* If this is not a SRV record, skip it. */
    if (type != ns_t_srv) {
      ptr += data_len;
      continue;
    }

    /* Read the priority. */
    int priority;
    GETSHORT(priority, ptr);

    /* Skip the weight. */
    ptr += 2;

    /* Read the port. */
    int port;
    GETSHORT(port, ptr);

    /* Read the target name. */
    char target[65536];
    name_len = dn_expand(job->answer, job->answer + job->answer_length, ptr, target, 65536);
    if (name_len < 0) uerror("dn_expand", Nothing);

    ptr += name_len;

    /* Create the OCaml value for this record. */
    record = caml_alloc_tuple(3);
    Store_field(record, 0, Val_int(priority));
    Store_field(record, 1, caml_copy_string(target));
    Store_field(record, 2, Val_int(port));
    node = caml_alloc_tuple(2);
    Field(node, 0) = record;
    Field(node, 1) = result;
    result = node;
  }

  CAMLreturn(result);
}

/* Free the job. */
CAMLprim value mlspot_service_lookup_free(value val_job)
{
  lwt_unix_free_job(&(Job_service_lookup_val(val_job))->job);
  return Val_unit;
}

#endif
