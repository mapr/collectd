#ifndef SRC_MAPR_METRICS_H
#define SRC_MAPR_METRICS_H
#ifdef __cplusplus
extern "C" {
#endif

#include "stdint.h"

typedef struct value_list_s value_list_t;

typedef struct MetricsBuffer {
  int32_t bytes;
  int32_t refcount;
  uint8_t data[0];
} MetricsBuffer;

// given a value_list_t *, decodes a MetricsBuffer * value stored in
// type_instance. Will return NULL if the vl doesn't point to opaque
// metric type.
MetricsBuffer * DecodeMetricsPointer(const value_list_t *vl);

// given a value_list_t * and a MetricsBuffer *, encodes MetricsBuffer * into
// type_instance of value_list_t * and populates the metric name.
// Encoding is human-friendly.
void EncodeMetricsPointer(value_list_t *vl, MetricsBuffer *ptr);

// allocates a buffer to store payload of a given size. Size must be 
// non-negative. Returns NULL on failure. Buffer *must* be eventually released
// by calling ReleaseMetricsPointer().
MetricsBuffer *AllocateMetricsPointer(int32_t bytes);

// releases a previously allocated or referenced buffer. Unless user called
// ReferenceMetricsPointer() on a buffer, it is UB to touch the buffer after
// calling ReleaseMetricsPointer() on it.
void ReleaseMetricsPointer(MetricsBuffer *buffer);

// increments the refcount on MetricsBuffer* allowing user to make copies.
void ReferenceMetricsPointer(MetricsBuffer *buffer);

#ifdef __cplusplus
}
#endif
#endif // SRC_MAPR_METRICS_H