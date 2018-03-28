#include "mapr_metrics.h"
#include "plugin.h"

#include <stdlib.h>

const char kOpaqueMetricName[] = "mapr_opaque";

static MetricsBuffer *Decoder(const char *encoded)
{
    uintptr_t decoded = 0;
    const char *position = &encoded[0];
    for (;;) {
      switch (*position)
      {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9': decoded += *position - '0'; break;
        case 'a':
        case 'b':
        case 'c':
        case 'd':
        case 'e':
        case 'f': decoded += *position - 'a' + 10; break;
        default: abort();
      }
      ++position;
      if (*position == '\0') {
        return (MetricsBuffer*)decoded;
      }
      decoded = decoded << 4;
    }
}


MetricsBuffer *DecodeMetricsPointer(const value_list_t *vl)
{
  if (strcmp(vl->type, kOpaqueMetricName) != 0) {
    return NULL;
  } else {
    return Decoder(vl->type_instance);
  }
}


void EncodeMetricsPointer(value_list_t *vl, MetricsBuffer *ptr)
{
  uintptr_t value = (uintptr_t) ptr;
  const int kChars = sizeof(MetricsBuffer*) * 8 / 4;
  char *position = &vl->type_instance[kChars - 1];

  vl->type_instance[kChars] = '\0';

  for (;;) {
    // take a digit
    uint8_t nibble = value & 0xf;
    //print it as hex
    if (nibble < 10) {
      *position = '0' + nibble;
    } else {
      *position = 'a' + nibble - 10;
    }
    if (position == &vl->type_instance[0]) {
      break;
    }
    --position;
    value = value >> 4;
  }

  strcpy(vl->type, kOpaqueMetricName);
}

MetricsBuffer *AllocateMetricsPointer(int32_t bytes)
{
  if (bytes < 0) {
    abort();
  } else {
    MetricsBuffer *buf = (MetricsBuffer*)malloc(sizeof(MetricsBuffer) + bytes);
    if (buf == NULL) {
      return NULL;
    }

    buf->bytes = bytes;
    buf->refcount = 1;
    return buf;
  }
}

void ReferenceMetricsPointer(MetricsBuffer *buffer)
{
  int32_t cautious = __sync_fetch_and_add(&buffer->refcount, 1);
  if (cautious <= 0) {
    abort();
  }
}

void ReleaseMetricsPointer(MetricsBuffer *buffer)
{
  int32_t paranoid = __sync_fetch_and_add(&buffer->refcount, -1);
  if (paranoid <= 0) {
    abort();
  }

  if (paranoid == 1) {
    free(buffer);
  }
}
