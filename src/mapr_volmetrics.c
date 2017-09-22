/**
 * collectd - src/mapr_vm.c
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 **/

#include "collectd.h"
#include "daemon/utils_time.h"
#include "common.h"
#include "plugin.h"
#include "configfile.h"
#include "stdio.h"
#include "sys/types.h"
#include "dirent.h"
#include "hdfs.h"

# include <glob.h>
# include <sys/stat.h>
# include <linux/limits.h>
# include <sys/sysinfo.h>
# if HAVE_LINUX_CONFIG_H
#   include <linux/config.h>
# endif
#include <stdint.h>
#include <assert.h>
#include <stdbool.h>

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <netdb.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>
#include <time.h>
#include <libgen.h>

#define XATTR_NAME "trusted.dispatchedOffset"
#define METRICS_DIRNAME "audit"
#define METRICS_FILE_PREFIX "Metrics.log"
#define METRICS_FILE_PREFIX_LEN (strlen(METRICS_FILE_PREFIX))
#define VOLLIST_FILE_PREFIX "Vollist_Metrics.log"
#define VOLLIST_FILE_PREFIX_LEN (strlen(VOLLIST_FILE_PREFIX))
#define MAX_READ_ITER 8

#define HASH_SEED 0x7fffffff

#define METRICS_PATH_PREFIX "/var/mapr/local"
#define PATH_MAX 4096
#define MAX_READ_SIZE (16*1024*1024)
#define READ_SIZE (128*1024)
#define VOLUME_NAME_MAX 256
#define METRICS_THRESHOLD (100*1024)

#define WINDOW_SIZE_SECONDS (60)
#define WINDOW_SIZE_MS (WINDOW_SIZE_SECONDS*1000)
#define WINDOW_MINUTE_MS (60*1000)
#define MAX_RECORDS_PER_ITER (10*1000)

/* Helpers go here */
inline uint64_t
murmurhash64(const void * key, int len)
{
  const uint64_t m = 0xc6a4a7935bd1e995ULL;
  const int r = 47; 

  uint64_t h = HASH_SEED ^ (len * m); 
  const uint64_t * data = (const uint64_t *)key;
  const uint64_t * end = data + (len/8);

  while(data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char * data2 = (const unsigned char*)data;

  switch(len & 7) {
    case 7: h ^= ((uint64_t)data2[6]) << 48; 
    case 6: h ^= ((uint64_t)data2[5]) << 40; 
    case 5: h ^= ((uint64_t)data2[4]) << 32; 
    case 4: h ^= ((uint64_t)data2[3]) << 24; 
    case 3: h ^= ((uint64_t)data2[2]) << 16; 
    case 2: h ^= ((uint64_t)data2[1]) << 8;
    case 1: h ^= ((uint64_t)data2[0]);
            h *= m;
  };  

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

static inline bool
startsWith(char *src, char *pattern, int plen)
{
  int i;
  for (i=0; i<plen; ++i) {
    if (src[i] == '\0' || pattern[i] != src[i]) {
      break;
    }
  }

  return (i == plen);
}

static inline char*
getNextDigit(char *record, char *end)
{
  while ((((*record) < '0') || ((*record) > '9')) && record <= end) {
    record++;
  }

  return record;
}

char*
getFileName(char *path)
{
  int i;
  int sawSlash = 0;
  char *ptr = NULL;

  for (i=0; i<4096 && path[i] != '\0'; i++) {
    if (path[i] == '/') {
      sawSlash = 1;
    } else if (sawSlash) {
      ptr = &path[i];
      sawSlash = 0;
    }
  }

  return ptr;
}

static int
isMetricsLogFile(hdfsFileInfo *fileInfo)
{
  if (fileInfo->mKind != kObjectKindFile) {
    return 0;
  }

  char *filename = getFileName(fileInfo->mName);
  if (startsWith(filename, (char *) VOLLIST_FILE_PREFIX,
      VOLLIST_FILE_PREFIX_LEN)) {
    return 1;
  }

  return 0;
}

static int
isMetricsAuditFile(hdfsFileInfo *fileInfo)
{
  if (fileInfo->mKind != kObjectKindFile) {
    return 0;
  }
 
  char *filename = getFileName(fileInfo->mName);
  if (startsWith(filename, (char *) METRICS_FILE_PREFIX,
                 METRICS_FILE_PREFIX_LEN)) {
    return 1;
  }

  return 0;
}

uint64_t millis_ = 0;
uint64_t mins_ = 0;
bool dispatch_ = true;

static uint64_t
getMillis(void) {
  return millis_;
}

static uint64_t
getMins(void) {
  return millis_/(WINDOW_SIZE_MS);
}

static uint64_t
currentTimeMillis(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);

  millis_ = ((tv.tv_sec * 1000) + (tv.tv_usec / 1000));
  return millis_;
}

char *bufHead_ = NULL;
int bufCount = 0;

char*
getBuffer(void)
{
  char *buf = NULL;

  if (bufHead_ == NULL) {
    buf = (char*)malloc(sizeof(char)*READ_SIZE);
    bufCount++;
  } else {
    buf = bufHead_;
    bufHead_ = *(char**)bufHead_;
  }

  return buf;
}

void
releaseBuffer(char *buffer)
{
  if (bufCount <= 100) {
    *(char**)buffer = bufHead_;
    bufHead_ = buffer;
  } else {
    free(buffer);
    bufCount--;
  }
}

pthread_mutex_t readLock;
int readInProgress_ = 0;

/*** Start of VolumeNameHashTable ***/

typedef struct VNHTEntry_ {
  uint32_t volId;
  char volumeName[VOLUME_NAME_MAX];
  struct VNHTEntry_ *next;
  struct VNHTEntry_ *fnext;
} VNHTEntry_t;

typedef struct {
  int nbuckets;
  int max_limit;
  uint32_t nentries;
  VNHTEntry_t **buckets;

  VNHTEntry_t *freeList;
  int fentries;
} VolumeNameHashTable;

static int
vnhtHashValue(VolumeNameHashTable *tbl, uint32_t volId)
{
  return (volId % tbl->nbuckets);
}

static void
vnhtInit(VolumeNameHashTable *tbl, int nbuckets, int limit)
{
  int i;

  tbl->nentries = 0;
  tbl->nbuckets = nbuckets;
  tbl->max_limit = limit;
  tbl->buckets = (VNHTEntry_t **)malloc(sizeof(VNHTEntry_t *)*nbuckets);
  for (i=0; i<nbuckets; ++i) {
    tbl->buckets[i] = NULL;
  }

  tbl->freeList = NULL;
  tbl->fentries = 0;
}

static void
vnhtInitEntry(VNHTEntry_t *entry)
{
  entry->volId = 0;
  entry->volumeName[0] = '\0';
  entry->next = NULL;
  entry->fnext = NULL;
}

static void
vnhtPutEntry(VolumeNameHashTable *tbl, VNHTEntry_t *entry)
{
  if (tbl->fentries < 1000) {
    entry->fnext = tbl->freeList;
    tbl->freeList = entry;
  } else {
    free(entry);
    tbl->fentries--;
  }
}

static VNHTEntry_t*
vnhtGetEntry(VolumeNameHashTable *tbl)
{
  VNHTEntry_t *entry = NULL;

  if (tbl->freeList) {
    entry = tbl->freeList;
    tbl->freeList = entry->fnext;
    entry->fnext = NULL;
  } else {
    entry = (VNHTEntry_t*)malloc(sizeof(VNHTEntry_t));
    vnhtInitEntry(entry);
    tbl->fentries++;
  }
  return entry;
}

static void
vnhtPurgeEntries(VolumeNameHashTable *tbl)
{
  int i;
  VNHTEntry_t *entry = NULL;
  VNHTEntry_t *next = NULL;
   
  for (i=0; i<tbl->nbuckets; i++) {
    entry = tbl->buckets[i];
    while (entry) {
      next = entry->next;
      entry->next = NULL;
      vnhtPutEntry(tbl, entry);
      entry = next;
    }
    tbl->buckets[i] = NULL;
  }
  tbl->nentries = 0;
}

static bool
shouldPurge(VolumeNameHashTable *tbl)
{
  /* Over a period of time as old files are deleted and new files
   * are created the table can get bigger in size. If it exceeds
   * a certain limit purge all the entries.
   */
  if (tbl->nentries > tbl->max_limit) {
    return true;
  }
  return false; 
}

static VNHTEntry_t*
vnhtLookup(VolumeNameHashTable *tbl, uint32_t volId)
{
  int hv = vnhtHashValue(tbl, volId);
  VNHTEntry_t *entry = tbl->buckets[hv];
  while (entry && entry->volId != volId) {
    entry = entry->next;
  }

  return entry;
}

static void
vnhtAdd(VolumeNameHashTable *tbl, uint32_t volId,
        const char *volName)
{
  int hv = vnhtHashValue(tbl, volId);
  VNHTEntry_t *entry = tbl->buckets[hv];
  while (entry && entry->volId != volId) {
    entry = entry->next;
  }

  if (!entry) {
    entry = vnhtGetEntry(tbl);
    entry->volId = volId;
    entry->next = tbl->buckets[hv];
    tbl->buckets[hv] = entry;
    tbl->nentries++;
  }
  strcpy(entry->volumeName, volName);
}

/*** End of VolumeNameHashTable ***/

/*** Start of MetricsHashTable ***/

#define HASH_BUCKETS (1023)

typedef enum {
  MetricsOpReadThroughput = 0,
  MetricsOpReadLatency,
  MetricsOpReadIOps,

  MetricsOpWriteThroughput,
  MetricsOpWriteLatency,
  MetricsOpWriteIOps,
  MetricsOpMAX
} MetricsOp;

typedef struct MHTEntry {
  uint32_t volId;
  uint64_t ts;//This will be multiple of WINDOW_SIZE_SECONDS
  double values[MetricsOpMAX];
  struct MHTEntry *next;
  struct MHTEntry *fnext;
} MHTEntry;

typedef struct {
  int nentries;
  int nbuckets;
  MHTEntry **buckets;
} MetricsHashTable;

typedef struct tsEntry_ {
  uint64_t ts;
  MetricsHashTable metricsTable;
  struct tsEntry_ *next;
  struct tsEntry_ *fnext;  
} tsEntry_t;

typedef struct TimestampHashTable_ {
  int nentries;
  int metricEntries;
  int nbuckets;
  int volbuckets;
  tsEntry_t **buckets;

  MHTEntry *freeMetricList;
  int fMetricentries;

  tsEntry_t *freeList;
  int fentries;
} TimestampHashTable;

static int
getMetricsHashValue(MetricsHashTable *tbl, uint32_t volId)
{
  return volId % tbl->nbuckets;
}

static void
initMetricsHashTable(MetricsHashTable *tbl, uint32_t nbuckets)
{
  int i;

  tbl->nbuckets = nbuckets;
  tbl->nentries = 0;
  tbl->buckets = (MHTEntry **)malloc(sizeof(MHTEntry *)*nbuckets);
  for (i=0; i<nbuckets; i++) {
    tbl->buckets[i] = NULL;
  }
}

static void
initMHTEntry(MHTEntry *entry)
{
  entry->volId = 0;
  entry->ts = 0;
  memset(entry->values, 0, sizeof(double)*MetricsOpMAX);
  entry->next = NULL;
  entry->fnext = NULL;
}

static void
populateMHTEntry(MHTEntry *entry, uint64_t ts, int volId,
                 uint32_t *values)
{
  entry->ts = ts;
  entry->volId = volId;

  int i;
  for (i=0; i<MetricsOpMAX; ++i) {
    entry->values[i] += values[i];
  }
}

static inline void
mhtPutEntry(TimestampHashTable *tbl, MHTEntry *entry)
{
  if (tbl->fMetricentries < 10000) {
    entry->fnext = tbl->freeMetricList;
    tbl->freeMetricList = entry;
  } else {
    free(entry);
    tbl->fMetricentries--;
  }
}

static inline MHTEntry*
mhtGetEntry(TimestampHashTable *tbl)
{
  MHTEntry *entry = NULL;

  if (tbl->freeMetricList) {
    entry = tbl->freeMetricList;
    tbl->freeMetricList = entry->fnext;
    memset(entry->values, 0, sizeof(double)*MetricsOpMAX);
    entry->fnext = NULL;
  } else {
    entry = (MHTEntry*)malloc(sizeof(MHTEntry));
    initMHTEntry(entry);
    tbl->fMetricentries++;
  }
  return entry;
}

static void
initTsEntry(tsEntry_t *entry)
{
  entry->ts = 0;
  entry->next = NULL;
  entry->fnext = NULL;
}

static int
getTsHashValue(TimestampHashTable* tbl, uint64_t ts)
{
  ts = ts - (ts % WINDOW_SIZE_MS);
  return ts % tbl->nbuckets;
}

static void
initTimestampHashTable(TimestampHashTable *tbl, int nbuckets,
                       int volbuckets)
{
  int i;

  tbl->nbuckets = nbuckets;
  tbl->volbuckets = volbuckets;
  tbl->buckets = malloc(sizeof(tsEntry_t)*nbuckets);
  for (i=0; i<nbuckets; i++) {
    tbl->buckets[i] = NULL;
  }

  tbl->fentries = 0;
  tbl->freeList = NULL;
  tbl->fMetricentries = 0;
  tbl->freeMetricList = NULL;
}

static void
tshtPutEntry(TimestampHashTable *tbl, tsEntry_t *entry)
{
  MetricsHashTable *mTbl = &(entry->metricsTable);
  assert(mTbl->nentries == 0);

  if (tbl->fentries < 1000) {
    entry->fnext = tbl->freeList;
    tbl->freeList = entry;
  } else {
    free(entry);
    tbl->fentries--;
  }
}

static tsEntry_t*
tshtGetEntry(TimestampHashTable *tbl)
{
  tsEntry_t *entry = NULL;

  if (tbl->freeList) {
    entry = tbl->freeList;
    tbl->freeList = entry->fnext;
    entry->fnext = NULL;
  } else {
    entry = (tsEntry_t*)malloc(sizeof(tsEntry_t));
    initTsEntry(entry);
    initMetricsHashTable(&entry->metricsTable, tbl->volbuckets); 
    tbl->fentries++;
  }
  return entry;
}

static void
tshtInsert(TimestampHashTable *tbl, uint32_t volId, uint64_t ts,
           double *values)
{
  int i;
  int hv;
  tsEntry_t *entry = NULL;
  tsEntry_t *new_entry = NULL;
  tsEntry_t *prev = NULL;
  uint64_t new_ts = ts - (ts % WINDOW_SIZE_MS);

  hv = getTsHashValue(tbl, ts);
  entry = tbl->buckets[hv];
  while (entry && new_ts > entry->ts) {
    prev = entry;
    entry = entry->next;
  }

  if ((entry && new_ts != entry->ts) ||
      !entry) {
    new_entry = tshtGetEntry(tbl);
    new_entry->ts = new_ts;
    if (prev) {
      prev->next = new_entry;
    } else {
      tbl->buckets[hv] = new_entry;
    }
    new_entry->next = entry;
    tbl->nentries++;
    entry = new_entry;
  }

  /* Now we have pointer to the Metrics hash table */
  MetricsHashTable *metricsTbl = &(entry->metricsTable);
  hv = getMetricsHashValue(metricsTbl, volId);
  MHTEntry* mEntry = metricsTbl->buckets[hv];
  while (mEntry && mEntry->volId != volId) {
    mEntry = mEntry->next;
  }
  
  if (!mEntry) {
    mEntry = mhtGetEntry(tbl);
    mEntry->ts = ts;
    mEntry->volId = volId;
    mEntry->next = metricsTbl->buckets[hv];
    metricsTbl->buckets[hv] = mEntry;
    metricsTbl->nentries++;
    tbl->metricEntries++;
  }

  for (i=0; i<MetricsOpMAX; ++i) {
    mEntry->values[i] += values[i];
  }
}

/*** End of MetricsHashTable ***/

/*** Start of FileMetricsHashTable ***/

typedef enum filetype_ {
  LOG = 1,
  AUDIT = 2
} filetype_t;

typedef struct FMHTEntry_ {
  uint64_t curOffset;
  int64_t dispatchedOffset;
  uint64_t minTs;
  uint64_t maxTs;
  struct FMHTEntry_ *next;
  struct FMHTEntry_ *fnext;
  filetype_t type;
  char filepath[PATH_MAX];
} FMHTEntry_t;

typedef struct {
  int nbuckets;
  int logfiles;
  int auditfiles;
  int maxlimit;
  FMHTEntry_t **buckets;

  FMHTEntry_t *freeList;
  int fentries;
} FileMetricsHashTable;

static void
fmhtInit(FileMetricsHashTable *tbl, int nbuckets, int maxlimit)
{
  int i;

  tbl->nbuckets = nbuckets;
  tbl->logfiles = 0;
  tbl->auditfiles = 0;
  tbl->maxlimit = maxlimit;
  tbl->buckets = (FMHTEntry_t **)malloc(sizeof(FMHTEntry_t *)*nbuckets);

  for (i=0; i<nbuckets; i++) {
    tbl->buckets[i] = NULL;
  }

  tbl->freeList = NULL;
  tbl->fentries = 0;
}

static void
fmhtInitEntry(FMHTEntry_t *entry)
{
  entry->filepath[0] ='\0';
  entry->curOffset = 0;
  entry->dispatchedOffset = 0;
  entry->minTs = 0;
  entry->maxTs = 0;
  entry->next = NULL;
  entry->fnext = NULL;
  entry->type = 0;
}

static void
fmhtPutEntry(FileMetricsHashTable *tbl, FMHTEntry_t *entry)
{
  if (tbl->fentries < 1000) {
    entry->fnext = tbl->freeList;
    tbl->freeList = entry;
  } else {
    free(entry);
    tbl->fentries--;
  }
}

static FMHTEntry_t*
fmhtGetEntry(FileMetricsHashTable *tbl)
{
  FMHTEntry_t *entry = NULL;

  if (tbl->freeList) {
    entry = tbl->freeList;
    tbl->freeList = entry->fnext;
    entry->fnext = NULL;
  } else {
    entry = (FMHTEntry_t*)malloc(sizeof(FMHTEntry_t));
    tbl->fentries++;
  }
  fmhtInitEntry(entry);
  return entry;
}

static void
fmhtPurgeEntries(FileMetricsHashTable *tbl, filetype_t type)
{
  int i;
  FMHTEntry_t *entry = NULL;
  FMHTEntry_t *prev = NULL;
  FMHTEntry_t *next = NULL;

  for (i=0; i<tbl->nbuckets; ++i) {
    entry = tbl->buckets[i];
    while (entry) {
      next = entry->next;
      if (entry->type == type) {
        if (!prev) {
          tbl->buckets[i] = entry->next;
        } else {
          prev->next = entry->next;
        }
        entry->next = NULL;
        fmhtPutEntry(tbl, entry);
        if (type == LOG) {
          tbl->logfiles--;
        } else {
          tbl->auditfiles--;
        }
      } else {
        prev = entry;
      }
      entry = next;
    }
  }
}

static int
fmhtHashValue(FileMetricsHashTable *tbl, const char *key)
{
  return (murmurhash64(key, strlen(key)) % tbl->nbuckets);
}

static FMHTEntry_t*
fmhtLookupOrInsert(FileMetricsHashTable *tbl, const char *key,
                   bool *created, filetype_t type)
{
  *created = false;
  int hv = fmhtHashValue(tbl, key);
  FMHTEntry_t *fmhtEntry = tbl->buckets[hv];
  while (fmhtEntry && strcmp(fmhtEntry->filepath, key)) {
    fmhtEntry = fmhtEntry->next;
  }

  if (!fmhtEntry) {
    fmhtEntry = fmhtGetEntry(tbl);
    strcpy(fmhtEntry->filepath, key);
    fmhtEntry->type = type;
    fmhtEntry->next = tbl->buckets[hv];
    tbl->buckets[hv] = fmhtEntry;
    if (type == LOG) {
      tbl->logfiles++;
    } else {
      tbl->auditfiles++;
    }
    *created = true;
  }

  return fmhtEntry;
}

/*** End of FileMetricsHashTable ***/

static FileMetricsHashTable fmht;
static VolumeNameHashTable vnht;
static TimestampHashTable tsht;
static bool htableInitialized = false;

static inline
MetricsOp getMetricsOp(char *record)
{
  bool isRead = ((*record) == 'R') ? true : false;
  record += 2;

  switch(*record) {
  case 'T':
    return (isRead) ? MetricsOpReadThroughput : MetricsOpWriteThroughput;

  case 'L':
    return (isRead) ? MetricsOpReadLatency : MetricsOpWriteLatency;

  case 'O':
    return (isRead) ? MetricsOpReadIOps : MetricsOpWriteIOps;
  }

  // We should never come here
  assert(0);
}

static int
dispatchMetrics(TimestampHashTable *tsTbl, tsEntry_t *tsEntry)
{
  int i;
  int j;
  int count = 0;
  uint64_t dispatchTs = tsEntry->ts;
  MetricsHashTable *tbl = &(tsEntry->metricsTable);
  MHTEntry *entry = NULL;
  MHTEntry *nextEntry = NULL;

  value_list_t vl = VALUE_LIST_INIT;
  value_t values[1];

  vl.time = MS_TO_CDTIME_T(dispatchTs);
  vl.values_len = 1;
  vl.values = values;
  sstrncpy(vl.plugin, "mapr.volmetrics", sizeof(vl.plugin));

  for (i=0; i<tbl->nbuckets; i++) {
    entry = tbl->buckets[i];

    VNHTEntry_t *vnhtEntry = NULL;
    if (entry) {
      vnhtEntry = vnhtLookup(&vnht, entry->volId);
    }

    while (entry) {
      nextEntry = entry->next;
      for (j= 0; j<MetricsOpMAX; j++) {
        if (vnhtEntry) {
          sprintf(vl.plugin_instance, "%s", vnhtEntry->volumeName);
        } else {
          sprintf(vl.plugin_instance, "%u", entry->volId);
        }

        if (entry->values[j] > 0) {
          char *type = NULL;
          double value = 0;
          switch(j) {
            case MetricsOpReadThroughput:
              type = "read_throughput";
              value = entry->values[j] / (double) WINDOW_SIZE_MS;
              break;
            case MetricsOpReadLatency:
              type = "read_latency";
              value = entry->values[j] / (double) entry->values[MetricsOpReadIOps];
              break;
            case MetricsOpReadIOps:
              type = "read_ops";
              value = entry->values[j];
              break;
            case MetricsOpWriteThroughput:
              type = "write_throughput";
              value = entry->values[j] / (double) WINDOW_SIZE_MS;
              break;
            case MetricsOpWriteLatency:
              type = "write_latency";
              value = entry->values[j] / (double) entry->values[MetricsOpWriteIOps];
              break;
            case MetricsOpWriteIOps:
              type = "write_ops";
              value = entry->values[j];
              break;
            default:
              // We should never come here
              assert(0);
          }

          sstrncpy(vl.type, type, sizeof(vl.type));
          vl.values[0].gauge = value;
          vl.values_len = 1;

          plugin_dispatch_values(&vl);
        }
      }
      mhtPutEntry(tsTbl, entry);
      tbl->nentries--;
      tsTbl->metricEntries--;
      entry = nextEntry;
      count++;
    }
    tbl->buckets[i] = NULL;
  }

  return count;
}

static int
syncOffset(hdfsFS fs, FMHTEntry_t *entry)
{
  char buf[32];
  if (entry->dispatchedOffset != entry->curOffset) {
    sprintf(&buf[0], "%ld", entry->curOffset);
    int err = hdfsSetXattr(fs, entry->filepath, XATTR_NAME,
                           strlen(XATTR_NAME), &buf[0], strlen(buf));
    if (err == -1) {
      WARNING("Error when setting xattr %s on %s: %s", XATTR_NAME,
              entry->filepath, strerror(errno));
      return -1;
    }

    INFO("Dispatched file %s offset to %lu", entry->filepath,
         entry->curOffset);

    entry->dispatchedOffset = entry->curOffset;
    entry->minTs = entry->maxTs;
  }
  return 0;
}

int
processBuffer(TimestampHashTable *tbl, char *buffer, int64_t bufLen,
              uint64_t *minTs, uint64_t *maxTs, bool *readAgain,
              int *records)
{
  /* format = {"ts":1502305800000,"vid":199334729,"RDT":43136.0,"RDL":732.0, 
   *           "RDO":338.0,"WRT":13888.0,"WRL":29.0,"WRO":28.0}
   */

  MetricsOp op;
  uint32_t volId = -1;
  uint64_t ts = -1;
  uint64_t prevTs = 0;
  char *start = buffer;
  char *end = buffer + bufLen - 1;
  uint32_t values[MetricsOpMAX];
  uint64_t millis = getMillis();
  uint64_t window = millis/WINDOW_SIZE_MS;
  uint64_t validTs = (window - 1) * WINDOW_SIZE_MS;
  int readBytes = 0;
  int saved_readBytes = 0;
  int ret;
  MHTEntry* mEntry = NULL;
  MHTEntry* prevMEntry = NULL;
  MHTEntry* savedMEntry = NULL;
  MHTEntry* headMEntry = NULL;

  *minTs = (uint64_t)-1;
  *maxTs = 0;
  *readAgain = true;
  *records = 0;

  while (*buffer != '\0') {
    if (*buffer == '}') {
      if (ts != -1 && volId != -1) {
        DEBUG("ts %lu, vid %u, RDT %u, RDL %u, RDO %u, WRT %u, WRL %u, WRO %u\n",
             ts, volId, values[MetricsOpReadThroughput],
             values[MetricsOpReadLatency], values[MetricsOpReadIOps],
             values[MetricsOpWriteThroughput], values[MetricsOpWriteLatency],
             values[MetricsOpWriteIOps]);

        if (ts > validTs) {
          *readAgain = false;
          saved_readBytes = readBytes;
          break;
        }

        if (prevTs && prevTs != ts) {
          *readAgain = false;
          saved_readBytes = readBytes;
          savedMEntry = prevMEntry;

          if (prevTs < *minTs) {
            *minTs = prevTs;
          }

          if (prevTs > *maxTs) {
            *maxTs = prevTs;
          }
        }

        mEntry = mhtGetEntry(tbl);
        populateMHTEntry(mEntry, ts, volId, values);
        if (!prevMEntry) {
          headMEntry = mEntry;
          prevMEntry = mEntry;
        } else {
          prevMEntry->next = mEntry;
        }
        mEntry->next = NULL;
        prevMEntry = mEntry;

        prevTs = ts;
        readBytes = (buffer - start + 1);
      }
    }

    switch(*buffer) {
      case 't':
        buffer = getNextDigit(buffer, end);
        if (buffer > end) {
          ret = bufLen;
          goto end;
        }
        ts = strtoull(buffer, &buffer, 10);
        break;

      case 'v':
        buffer = getNextDigit(buffer, end);
        if (buffer > end) {
          ret = bufLen;
          goto end;
        }
        volId = strtoul(buffer, &buffer, 10);
        break;

      case 'R':
      case 'W':
        op = getMetricsOp(buffer);
        buffer = getNextDigit(buffer, end);
        if (buffer > end) {
          ret = bufLen;
          goto end;
        }
        values[op] = strtoul(buffer, &buffer, 10);
        break;

      default:
        break;
    }

    buffer++;
  }

  /* Yeah too much of walking the lists */
  MHTEntry *nextMEntry = NULL;
  for (mEntry=headMEntry; mEntry; mEntry=nextMEntry) {
    nextMEntry = mEntry->next;
    (*records)++;
    tshtInsert(tbl, mEntry->volId, mEntry->ts, mEntry->values);
    if (mEntry == savedMEntry) {
      mEntry = nextMEntry;
      break;
    }
    free(mEntry);
  }

  for (; mEntry; mEntry=nextMEntry) {
    nextMEntry = mEntry->next;
    free(mEntry);
  }

  if (!savedMEntry) {
    *minTs = *maxTs = prevTs;
  }

  if (!(*readAgain)) {
    ret = saved_readBytes;
  } else {
    ret = bufLen;
  }

end:
  INFO("minTs %lu maxTs %lu readAgain %d ret %d records %d", *minTs, *maxTs, 
        *readAgain, ret, *records); 
  return ret;
}

static int64_t
readMetrics(TimestampHashTable *tbl, hdfsFS fs, FMHTEntry_t *fmhtEntry,
            hdfsFileInfo *fileInfo)
{
  int j;
  int ret = 0;
  uint64_t minTs;
  uint64_t maxTs;
  tSize actualLen = 0;
  tSize readBytes = 0;
  tSize totalRead = 0;
  hdfsFile file;
  bool readAgain = false;
  int records = 0;

  char *buffer = getBuffer();
  if (!buffer) {
    ERROR("Could not allocate memory for the read buffer");
    return -ENOMEM;
  }

  /* We are opening the file and closing it every time because of the
   * attr timeout issues.
   */
  file = hdfsOpenFile(fs, fmhtEntry->filepath, O_RDONLY, 0, 0, 0);
  if (!file) {
    ERROR( "Failed to open %s for reading, errno %d\n", fmhtEntry->filepath,
           errno);
    releaseBuffer(buffer);
    return -errno;
  }

  do {
    /* TODO: Pread or seek and read. Throw away left over or save it.
     * Will throwing away cause unnecessary decompression issues on server.
     */
    readBytes = hdfsPread(fs, file, fmhtEntry->curOffset, buffer, READ_SIZE);
    if (readBytes <= 0) {
      INFO("hdfsPread failed for file: %s, offset: %ld, errno %d readBytes %d",
           fmhtEntry->filepath, fmhtEntry->curOffset, errno, readBytes);
      if (readBytes < 0) {
        totalRead = -errno;
      }
      goto end;
    } 
  
    for (j=0; j<readBytes; j++) {
      if (buffer[j] == '{') {
        break;
      }
    }

    if (buffer[j] != '{') {
      fmhtEntry->curOffset += readBytes;
      totalRead += readBytes;
      break;
    }

    while (readBytes > 0 && buffer[readBytes - 1] != '}') {
      readBytes--;
    }

    actualLen = readBytes - j; 
    if (actualLen <= 0) {
      /* Throw away the data. Reread later */
      readBytes = actualLen;
      goto end;
    }

    buffer[readBytes] = '\0';
    ret = processBuffer(tbl, &buffer[j], actualLen + 1, &minTs, &maxTs,
                        &readAgain, &records);
    if (ret <= 0) {
      goto end;
    }

    INFO("Read audit file %s curoffset %lu consumed %d readBytes %d\n",
         fmhtEntry->filepath, fmhtEntry->curOffset, j + ret,
         readBytes);

    totalRead += j + ret;
    fmhtEntry->curOffset = fmhtEntry->curOffset + j + ret;
    if (records) {
      fmhtEntry->minTs = minTs;
      fmhtEntry->maxTs = fmhtEntry->maxTs < maxTs ? maxTs : fmhtEntry->maxTs;
    }
  } while (readAgain);

end:
  hdfsCloseFile(fs, file);
  releaseBuffer(buffer);
  return totalRead; 
}

/* Pick the file with the min maxTs.
 * Also, ignore files which have nothing to be read.
 */
static FMHTEntry_t*
pickFile(FileMetricsHashTable *fmht, hdfsFS fs,
         hdfsFileInfo *fileList, int numFiles,
         int *fileId)
{
  int i;
  bool created;
  uint64_t minTs = (uint64_t)-1;
  FMHTEntry_t *fmhtEntry = NULL;
  FMHTEntry_t *min = NULL;

  *fileId = -1;
  for (i=0; i<numFiles; i++) {
    if (isMetricsAuditFile(&fileList[i])) {
      fmhtEntry = fmhtLookupOrInsert(fmht, fileList[i].mName,
                                     &created, AUDIT);

      /* If the entry just got created let's get the offset from xattr */
      /* See if this can be moved to a function */
      if (created) {
        char buf[32];
        int xattrSize = hdfsGetXattr(fs, fileList[i].mName, XATTR_NAME,
                                         &buf[0], 32);
        if (xattrSize <= 0) {
          ERROR("Error when getting xattr %s on %s, errno %d", XATTR_NAME,
                fileList[i].mName, errno);
          fmhtEntry->curOffset = 0;
          fmhtEntry->dispatchedOffset = 0;
        } else {
          buf[xattrSize] = '\0';
          fmhtEntry->dispatchedOffset = strtoll((const char *)&buf[0], NULL, 10);
          fmhtEntry->curOffset = fmhtEntry->dispatchedOffset;
        }
      }

      if (fmhtEntry->curOffset < fileList[i].mSize) {
        if (fmhtEntry->maxTs < minTs) {
          minTs = fmhtEntry->maxTs;
          min = fmhtEntry;
          *fileId = i;
        }
      }
    }
  }

  return min;
}

static uint64_t
getDispatchTs(hdfsFileInfo *fileList, int numFiles, FMHTEntry_t **retEntry)
{
  int i;
  bool created = false;
  FMHTEntry_t *fmhtEntry = NULL;
  uint64_t minTs = (uint64_t)-1;;
  uint64_t millis = getMillis();
  uint64_t curWindow = millis/WINDOW_SIZE_SECONDS;
  uint64_t dispatchTs = (curWindow - 1)*WINDOW_SIZE_SECONDS;

  *retEntry = NULL;
  for (i=0; i<numFiles; i++) {
    if (isMetricsAuditFile(&fileList[i])) {
      fmhtEntry = fmhtLookupOrInsert(&fmht, fileList[i].mName,
                                     &created, AUDIT);
      if (fmhtEntry) {
        if (fmhtEntry->dispatchedOffset < fmhtEntry->curOffset &&
            fmhtEntry->maxTs < minTs && fmhtEntry->maxTs <= dispatchTs) {
          minTs = fmhtEntry->maxTs;
          *retEntry = fmhtEntry;
        }
      }
    }
  }

  return minTs;
}

/* Either clock struck a new minute or the table size
 * got too big.
 */
static bool
dispatchNow(TimestampHashTable *tbl)
{
  if (dispatch_) {
    return true;
  } else if (tbl->metricEntries > METRICS_THRESHOLD) {
    return true;
  }
  return false;
}

static void
processAuditFiles(FileMetricsHashTable *fmht, TimestampHashTable *tsTbl,
                  hdfsFS fs, hdfsFileInfo *fileList, int numFiles)
{
  int i;
  int ret = 0;
  int fileid = 0;
  int count = 0;
  FMHTEntry_t *fmhtEntry = NULL;
  int totalRead = 0;
  FMHTEntry_t *dispatchEntry = NULL;
  tsEntry_t *next = NULL;
  bool dispatch = false;
  uint64_t dispatchTs = 0;
  int totalFiles = numFiles;

  while (totalRead < MAX_READ_SIZE) {
    /* Pick a file to read.
     * Read the metrics.
     * Dispatch the metrics.
     */
    fmhtEntry = pickFile(fmht, fs, fileList, totalFiles, &fileid);
    if (fmhtEntry) {
      ret = readMetrics(tsTbl, fs, fmhtEntry, &fileList[fileid]);
      INFO("Picked audit file %s for processing, curOffset %lu size %lu "
           "consumed %d", fileList[fileid].mName, fmhtEntry->curOffset,
           fileList[fileid].mSize, ret);
      if (ret <= 0) {
        fileList[fileid] = fileList[totalFiles-1];
        totalFiles--;
        continue;
      }
      totalRead += ret;
    } else {
      break;
    }
  }

  while (count < MAX_RECORDS_PER_ITER) {
    dispatch = dispatchNow(tsTbl);
    if (dispatch) {
      /* Get a timestamp which is safe to dispatch */
      dispatchTs = getDispatchTs(fileList, numFiles, &dispatchEntry);
      if (dispatchEntry != NULL) {
        for (i=0; i<tsTbl->nbuckets; i++) {
          tsEntry_t *entry = tsTbl->buckets[i];
          while (entry) {
            if (entry->ts <= dispatchTs) {
              count += dispatchMetrics(tsTbl, entry);
            } else {
              break;
            }
            tsTbl->buckets[i] = entry->next;
            next = entry->next;
            tshtPutEntry(tsTbl, entry);
            tsTbl->nentries--;
            entry = next;
          }
        }
        syncOffset(fs, dispatchEntry);
      } else {
        break;
      }
    } else {
      break;
    }
  }
}

static int
readAndCacheVolumeNames(hdfsFS fs, FMHTEntry_t *fmhtEntry)
{
  uint32_t volId;
  char *line;
  tSize readBytes = 0;
  char *lineSavePtr;
  char *volSavePtr;
  hdfsFile file;
  int j;

  char *buffer = getBuffer();
  if (!buffer) {
    ERROR("Could not allocate memory for the read buffer");
    return -ENOMEM;
  }

  file = hdfsOpenFile(fs, fmhtEntry->filepath, O_RDONLY, 0, 0, 0);
  if (!file) {
    ERROR("Failed to open %s for reading, errno %d\n", fmhtEntry->filepath,
          errno);
    releaseBuffer(buffer);
    return -errno;
  }

  while (true) {
    readBytes = hdfsPread(fs, file, fmhtEntry->curOffset, buffer, READ_SIZE);
    if (readBytes < 0) {
      WARNING("hdfsPread failed for file: %s, offset: %ld, errno %d",
              fmhtEntry->filepath, fmhtEntry->curOffset, errno);
      readBytes = -errno;
      goto end;
    } else if (readBytes == 0) {
      goto end;
    }

    buffer[readBytes] = '\0';

    line = strtok_r(buffer, "\n", &lineSavePtr);
    while (line) {
      char *volIdStr = strtok_r(line, ",", &volSavePtr);
      if (!volIdStr) {
        WARNING("Bad line. Volume Id not found.");
      } else {
        volId = strtoul(volIdStr, NULL, 10);
        char *volName = strtok_r(NULL, ",", &volSavePtr);
        if (volName == NULL) {
          WARNING("Bad line. Volume name not found.");
        } else {
          INFO("Adding volume %u with name %s to cache ", volId, volName);
          vnhtAdd(&vnht, volId, volName);
        }
      }
      line = strtok_r(NULL, "\n", &lineSavePtr);
    }

    /* Throw away the partial data from the previous read */
    for (j=readBytes-1; j>=0; j--) {
      if (buffer[j] == '\n') {
        readBytes = j+1;
        break;
      }
    }
   
    buffer[readBytes] = '\0'; 
    fmhtEntry->curOffset += readBytes;
  }

end:
  hdfsCloseFile(fs, file);
  releaseBuffer(buffer);
  return readBytes;
}

static void
processLogFiles(FileMetricsHashTable *fmht, hdfsFS fs, hdfsFileInfo *fileList,
                int numFiles)
{
  int i;
  bool created = false;
  FMHTEntry_t *fmhtEntry = NULL;

  for (i=0; i<numFiles; i++) {
    if (isMetricsLogFile(&fileList[i])) {
      /* Metrics log file maintains a map of volume id to volume name.
       * Since the audit file is going to have the volume id we might to
       * convert the volume id to volume name before pushing it to opentsdb.
       */
      fmhtEntry = fmhtLookupOrInsert(fmht, fileList[i].mName, &created, LOG);
      if (fmhtEntry->curOffset < fileList[i].mSize) {
        INFO("Processing log file %s offset %lu size %lu", fileList[i].mName,
              fmhtEntry->curOffset, fileList[i].mSize);
        readAndCacheVolumeNames(fs, fmhtEntry);
      }
    }
  }
}

static int
shouldPurgeFMHT(FileMetricsHashTable *fmht, TimestampHashTable *tsTbl)
{
  if (fmht->auditfiles > fmht->maxlimit) {
    return true;
  } else if (tsTbl->metricEntries > METRICS_THRESHOLD) {
    return true;
  }
  return false;
}

static void
PurgeFMHTCache(FileMetricsHashTable *fmht)
{
  int i;
  FMHTEntry_t *entry = NULL;
  FMHTEntry_t *next = NULL;

  for (i=0; i<fmht->nbuckets; i++) {
    entry = fmht->buckets[i];
    while (entry) {
      next = entry->next;
      if (entry->dispatchedOffset == entry->curOffset) {
        fmhtPutEntry(fmht, entry);
      }
      entry = next;
    }
  }
}

static void
PurgeVolumeCache(void)
{
  /* Purge all the log files from the file cache and volume names
   * from the volume name cache.
   */  
  vnhtPurgeEntries(&vnht);
  fmhtPurgeEntries(&fmht, LOG);
}

static inline int
directoryExists(hdfsFS fs, char *metricsDir)
{
  hdfsFileInfo *info = hdfsGetPathInfo(fs, metricsDir);
  if (!info) {
    return -1;
  } else if (info->mKind != kObjectKindDirectory) {
    hdfsFreeFileInfo(info, 1);
    return -1;
  }

  hdfsFreeFileInfo(info, 1);
  return 0;
}

static inline int
getMetricsPath(char *metricsDir)
{
  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);
  struct hostent *h;
  h = gethostbyname(hostname);

  sprintf(&metricsDir[0], "%s/%s/%s", METRICS_PATH_PREFIX, h->h_name,
           METRICS_DIRNAME);

  DEBUG("Metrics path is %s ", metricsDir);
  return 0;
}

hdfsFS fs = NULL;

hdfsFS
connectCluster(void)
{
  hdfsFS fs = hdfsConnect("default", 0);
  if (!fs ) {
    ERROR("Oops! Failed to connect to hdfs!\n");
    return NULL;
  }

  return fs;
}

static inline int
readInProgress(void)
{
  pthread_mutex_lock(&readLock);
  if (readInProgress_) {
    pthread_mutex_unlock(&readLock);
    return 1;
  }
  readInProgress_ = 1;
  pthread_mutex_unlock(&readLock);

  return 0;
}

typedef struct dirlist_ {
  hdfsFileInfo *entries;
  int numFiles;
  struct dirlist_ *next;
} dirlist_t;

int
compare(const void *x, const void *y)
{
  hdfsFileInfo *a = (hdfsFileInfo*)x;
  hdfsFileInfo *b = (hdfsFileInfo*)y;
  return a->mLastMod - b->mLastMod;
}

static int
vm_read(void)
{
  char metricsDir[1024];
  int err = 0;
  int i = 0;
  int numEntries = 0;

  /* Let's make sure only one thread runs at a time */
  if (readInProgress()) {
    return 0;
  }

  currentTimeMillis();
  if (mins_ != getMins()) {
    mins_ = getMins();
    dispatch_ = true;
  } else {
    dispatch_ = false;
  }

  /* Let's try to reuse the fs handle */
  if (!fs) {
    fs = connectCluster();
    if (!fs) {
      return -1;
    }
  }

  err = getMetricsPath(metricsDir);
  if (err) {
    return -1;
  }

  err = directoryExists(fs, metricsDir);
  if (err) {
    return -1;
  }

  /* Volume name cache could be holding too many volume name entries
   * in memory. Let's see if it got too big and purge
   * the cache.
   */
  if (shouldPurge(&vnht)) {
    PurgeVolumeCache();
  }

  dirlist_t *head = NULL;
  dirlist_t *cur = NULL;
  dirlist_t *next = NULL;
  int numFiles = 0;
  int totalFiles = 0;
  int j;

  hdfsFileInfo *dirList = hdfsListDirectory(fs, metricsDir, &numEntries);
  for (i=0; i<numEntries; ++i) {
    /* Let's walk over each mfs instance */ 
    if (dirList[i].mKind == kObjectKindDirectory) {
      hdfsFileInfo *fileList = hdfsListDirectory(fs, dirList[i].mName,
                                                 &numFiles);
      if (numFiles) {
        dirlist_t *entry = (dirlist_t*) malloc(sizeof(dirlist_t));
        entry->entries = fileList;
        entry->numFiles = numFiles;
        entry->next = head;
        head = entry;
        totalFiles += numFiles;
      }
    }
  }

  if (totalFiles) {
    /* Flatten the directory structure and create list of files */
    hdfsFileInfo *fileList = (hdfsFileInfo*)malloc(sizeof(hdfsFileInfo)
        *totalFiles);

    for (cur=head, j=0; cur; cur=cur->next) {
      for (i=0; i<cur->numFiles; i++) {
        fileList[j] = cur->entries[i];
        j++; 
      }
    }

    qsort(fileList, totalFiles, sizeof(hdfsFileInfo), compare);
    processLogFiles(&fmht, fs, fileList, totalFiles);
    processAuditFiles(&fmht, &tsht, fs, fileList, totalFiles);

    for (next=NULL; head; head=next) {
      next = head->next;
      hdfsFreeFileInfo(head->entries, head->numFiles);
      free(head);
    }
    hdfsFreeFileInfo(dirList, numEntries);
  }

  if (shouldPurgeFMHT(&fmht, &tsht)) {
    PurgeFMHTCache(&fmht);
  }

  assert(head == NULL);
  readInProgress_ = 0;
  return 0;
}

//TODO: can we take the aggregation time from the config? - Sriram
//Since this is going to run in mfs do we have to make sure 
//that meaningful client config can be read from here
static int
vm_config(oconfig_item_t *ci) {
  return 0;
}

/* TODO: Will this function get called multiple times ? */
static int
vm_init(void) {
  if (!htableInitialized) {
    htableInitialized = true;
    fmhtInit(&fmht, 11, 20);
    vnhtInit(&vnht, 511, 10240);
    initTimestampHashTable(&tsht, 101, 1279);
  }

  int err = pthread_mutex_init(&readLock, NULL);
  if (err) {
    ERROR("mutex could not be initialized, errno %d ", err);
    return err;
  }
  readInProgress_ = 0;
  return 0;
}

void module_register(void)
{
  plugin_register_complex_config("mapr_volmetrics", vm_config);
  plugin_register_init("mapr_volmetrics", vm_init);
  plugin_register_read("mapr_volmetrics", vm_read);
} /* void module_register */
