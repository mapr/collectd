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
#define BUF_SIZE 4096
#define MAX_READ_ITER 10

#define HASH_SEED 0x7fffffff
inline uint64_t murmurhash64(const void * key, int len)
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

/*** Start of VolumeNameHashTable ***/

typedef struct VNHTEntry {
  uint32_t volId;
  char volumeName[256];
  struct VNHTEntry *next;
} VNHTEntry;

typedef struct {
  int nbuckets;
  uint32_t nentries;
  VNHTEntry **entries;
} VolumeNameHashTable;

static int vnhtHashValue(VolumeNameHashTable *tbl, uint32_t volId)
{
  return (volId % tbl->nbuckets);
}

static void vnhtInit(VolumeNameHashTable *tbl, int nbuckets)
{
  tbl->nentries = 0;
  tbl->nbuckets = nbuckets;
  tbl->entries = (VNHTEntry **)malloc(sizeof(VNHTEntry *) * nbuckets);
  int i;
  for (i = 0; i < nbuckets; ++i)
    tbl->entries[i] = NULL;
}

static VNHTEntry *vnhtLookup(VolumeNameHashTable *tbl, uint32_t volId)
{
  int hv = vnhtHashValue(tbl, volId);
  VNHTEntry *entry = tbl->entries[hv];
  while (entry && entry->volId != volId)
    entry = entry->next;

  return entry;
}

static void vnhtAdd(VolumeNameHashTable *tbl, uint32_t volId,
              const char *volName)
{
  int hv = vnhtHashValue(tbl, volId);
  VNHTEntry *entry = tbl->entries[hv];
  while (entry && entry->volId != volId)
    entry = entry->next;

  if (!entry) {
    entry = (VNHTEntry *)malloc(sizeof(VNHTEntry));
    entry->volId = volId;
    entry->next = tbl->entries[hv];
    tbl->entries[hv] = entry;
    tbl->nentries++;
  }
  strcpy(entry->volumeName, volName);
}

/*** End of VolumeNameHashTable ***/

/*** Start of MetricsHashTable ***/

#define WINDOW_SIZE_SECONDS (60)
#define WINDOW_SIZE_MS (60000)
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

struct FileMetricsHashTable;

typedef struct MHTEntry {
  uint32_t volId;
  uint64_t ts;      // This will be multiple of WINDOW_SIZE_SECONDS
  double values[MetricsOpMAX];
  struct MHTEntry *next;
} MHTEntry;

typedef struct {
  int nentries;
  int nbuckets;
  struct FileMetricsHashTable *parentFMHT;
  MHTEntry **entries;
} MetricsHashTable;

static int getHashValue(MetricsHashTable *tbl, uint32_t volId, uint64_t ts)
{
  int v1 = volId % tbl->nbuckets;
  int v2 = ts % tbl->nbuckets;
  return ((v1 + v2) % tbl->nbuckets);
}

static void initMetricsHashTable(MetricsHashTable *tbl, uint32_t nbuckets)
{
  tbl->nbuckets = nbuckets;
  tbl->nentries = 0;
  tbl->entries = (MHTEntry **)malloc(sizeof(MHTEntry *) * nbuckets);
  int i;
  for (i = 0; i < nbuckets; i++) {
    tbl->entries[i] = NULL;
  }
}

//TODO: use a slab for MHTEntry - Sriram
static inline void freeMHTEntry(MHTEntry *entry)
{
  free(entry);
}

static inline MHTEntry *allocateMHTEntry()
{
  MHTEntry *entry = (MHTEntry *)calloc(sizeof(MHTEntry), 1);
  return entry;
}

static void mhtInsert(MetricsHashTable *tbl, uint32_t volId, uint64_t ts,
                          uint32_t *values)
{
  int hv = getHashValue(tbl, volId, ts);
  MHTEntry *entry = tbl->entries[hv];
  while (entry && (entry->volId != volId || entry->ts != ts)) {
    entry = entry->next;
  }

  if (!entry) {
    entry = allocateMHTEntry();
    entry->volId = volId;
    entry->ts = ts;
    entry->next = tbl->entries[hv];
    tbl->entries[hv] = entry;
    tbl->nentries++;
  }

  int i;
  for (i = 0; i < MetricsOpMAX; ++i) {
    entry->values[i] += values[i];
  }
}

static MHTEntry *getMHTEntriesWithTs(MetricsHashTable *tbl, uint64_t ts)
{
  if (tbl->nentries == 0) return NULL;

  MHTEntry *entryList = NULL;
  int i;
  for (i = 0; i < tbl->nbuckets; ++i) {
    MHTEntry *prevEntry = NULL, *nextEntry = NULL;
    MHTEntry *entry = tbl->entries[i];
    while (entry) {
      nextEntry = entry->next;

      // Remove from the hash table and add to entryList. Make sure the bucket
      // head is set properly
      if (entry->ts == ts) {
        if (prevEntry) {
          prevEntry->next = nextEntry;
        } else {
          tbl->entries[i] = nextEntry;
        }

        entry->next = entryList;
        entryList = entry;

        tbl->nentries--;
      } else {
        prevEntry = entry;
      }

      entry = nextEntry;
    }
  }

  return entryList;
}

/*** End of MetricsHashTable ***/

/*** Start of FileMetricsHashTable ***/

typedef struct FMHTEntry {
  char filepath[1024];
  uint64_t curDispatchTs;
  int64_t curOffset;
  int64_t dispatchedOffset;
  struct tm cachedMtime;
  MetricsHashTable mht;
  struct FMHTEntry *next;
} FMHTEntry;

typedef struct {
  int nbuckets;
  int nentries;
  FMHTEntry **entries;
} FileMetricsHashTable;

static void fmhtInit(FileMetricsHashTable *fmht, int nbuckets)
{
  fmht->nbuckets = nbuckets;
  fmht->nentries = 0;
  fmht->entries = (FMHTEntry **)malloc(sizeof(FMHTEntry *) * nbuckets);
  int i;
  for (i = 0; i < nbuckets; ++i)
    fmht->entries[i] = NULL;
}

static int fmhtHashValue(FileMetricsHashTable *tbl, const char *key)
{
  return (murmurhash64(key, strlen(key)) % tbl->nbuckets);
}

static FMHTEntry *allocateFMHTEntry()
{
  FMHTEntry *fmhtEntry = (FMHTEntry *)malloc(sizeof(FMHTEntry));
  fmhtEntry->curDispatchTs = 0;
  fmhtEntry->curOffset = 0;
  fmhtEntry->dispatchedOffset = 0;
  initMetricsHashTable(&fmhtEntry->mht, HASH_BUCKETS);
  return fmhtEntry;
}

static FMHTEntry *fmhtLookupOrInsert(FileMetricsHashTable *tbl, const char *key,
                    bool *created)
{
  *created = false;
  int hv = fmhtHashValue(tbl, key);
  FMHTEntry *fmhtEntry = tbl->entries[hv];
  while (fmhtEntry && strcmp(fmhtEntry->filepath, key))
    fmhtEntry = fmhtEntry->next;

  if (!fmhtEntry) {
    fmhtEntry = allocateFMHTEntry();
    strcpy(fmhtEntry->filepath, key);
    fmhtEntry->next = tbl->entries[hv];
    tbl->entries[hv] = fmhtEntry;
    tbl->nentries++;
    *created = true;
  }

  return fmhtEntry;
}

static void fmhtRemove(FileMetricsHashTable *tbl, const char *key)
{
  int hv = fmhtHashValue(tbl, key);
  FMHTEntry *fmhtEntry = tbl->entries[hv], *prevFMHTEntry = NULL;
  while (fmhtEntry && strcmp(fmhtEntry->filepath, key)) {
    prevFMHTEntry = fmhtEntry;
    fmhtEntry = fmhtEntry->next;
  }

  if (fmhtEntry) {
    if (prevFMHTEntry) {
      prevFMHTEntry->next = fmhtEntry->next;
    } else {
      tbl->entries[hv] = fmhtEntry->next;
    }

    //TODO: make sure we release the memory held by metricshashtable
    free(fmhtEntry);
  }
}

/*** End of FileMetricsHashTable ***/

static FileMetricsHashTable fmht;
static VolumeNameHashTable vnht;
static bool htableInitialized = false;

//TODO: can we take the aggregation time from the config? - Sriram
static int vm_config(oconfig_item_t *ci) {
  INFO("Inside %s", __func__);
  return 0;
}

static int vm_init(void) {
  if (!htableInitialized) {
    INFO("Inside %s", __func__);
    htableInitialized = true;
    fmhtInit(&fmht, 11);
    vnhtInit(&vnht, 511);
  }

  return 0;
}

static void dispatchEntries(hdfsFS fs, FMHTEntry *fmhtEntry,
              int64_t newDispatchedOffset, uint64_t newDispatchTs)
{
  int64_t oldDispatchedOffset = fmhtEntry->dispatchedOffset;
  MHTEntry *entry = getMHTEntriesWithTs(&fmhtEntry->mht,
                      fmhtEntry->curDispatchTs);
  int i;
  value_list_t vl = VALUE_LIST_INIT;
  value_t values[1];

  vl.time = MS_TO_CDTIME_T(fmhtEntry->curDispatchTs);
  vl.values_len = 1;
  vl.values = values;
  sstrncpy(vl.plugin, "mapr.volmetrics", sizeof(vl.plugin));

  MHTEntry *nextEntry = NULL;
  while (entry) {
    nextEntry = entry->next;
    for (i = 0; i < MetricsOpMAX; ++i) {
      VNHTEntry *vnhtEntry = vnhtLookup(&vnht, entry->volId);
      if (vnhtEntry) {
        sprintf(vl.plugin_instance, "%s", vnhtEntry->volumeName);
      } else {
        sprintf(vl.plugin_instance, "%u", entry->volId);
      }
      if (entry->values[i] > 0) {
        //TODO: change this to set the volume name instead of volume id - Sriram

        char *type = NULL;
        double value = 0;
        switch(i) {
          case MetricsOpReadThroughput:
            type = "read_throughput";
            value = entry->values[i] / (double) WINDOW_SIZE_SECONDS;
            break;
          case MetricsOpReadLatency:
            type = "read_latency";
            value = entry->values[i] / (double) entry->values[MetricsOpReadIOps];
            break;
          case MetricsOpReadIOps:
            type = "read_ops";
            value = entry->values[i];
            break;
          case MetricsOpWriteThroughput:
            type = "write_throughput";
            value = entry->values[i] / (double) WINDOW_SIZE_SECONDS;
            break;
          case MetricsOpWriteLatency:
            type = "write_latency";
            value = entry->values[i] / (double) entry->values[MetricsOpWriteIOps];
            break;
          case MetricsOpWriteIOps:
            type = "write_ops";
            value = entry->values[i];
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

    freeMHTEntry(entry);
    entry = nextEntry;
  }

  fmhtEntry->curDispatchTs = newDispatchTs;
  fmhtEntry->dispatchedOffset = newDispatchedOffset;

  if (oldDispatchedOffset != fmhtEntry->dispatchedOffset) {
    char buf[32];
    sprintf(&buf[0], "%ld", fmhtEntry->dispatchedOffset);
    int err = hdfsSetXattr(fs, fmhtEntry->filepath, XATTR_NAME,
        strlen(XATTR_NAME), &buf[0], strlen(buf));
    if (err == -1) {
      ERROR("Error when setting xattr %s on %s: %s", XATTR_NAME,
          fmhtEntry->filepath, strerror(errno));
    }
  }
}

static inline char *getNextDigit(char *record)
{
  while ((((*record) < '0') || ((*record) > '9')) && *record != '\0')
    record++;

  return record;
}


static inline MetricsOp getMetricsOp(char *record)
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

static inline bool startsWith(char *src, char *pattern, int plen)
{
  int i;
  for (i = 0; i < plen; ++i) {
    if (src[i] == '\0' || pattern[i] != src[i])
      break;
  }

  return (i == plen);
}

void processRecord(hdfsFS fs, FMHTEntry *fmhtEntry, char **buffer,
                    int64_t bufOffset)
{
  // format = "{\"ts\":1502305800000,\"vid\":199334729,\"RDT\":43136.0,\"RDL\":732.0,\"RDO\":338.0,\"WRT\":13888.0,\"WRL\":29.0,\"WRO\":28.0}";

  MetricsOp op;
  uint32_t volId = -1;
  uint64_t ts = -1;
  uint32_t values[MetricsOpMAX];
  char *record = *buffer;

  while ((*record) != '\0') {
    if ((*record) == '\n') {
      record++;
      continue;
    }

    switch(*record) {
    case 't':
      record = getNextDigit(record);
      ts = strtoull(record, &record, 10);
      ts = ts - (ts % WINDOW_SIZE_MS);
      if (fmhtEntry->curDispatchTs == 0)
        fmhtEntry->curDispatchTs = ts;
      break;

    case 'v':
      record = getNextDigit(record);
      volId = strtoul(record, &record, 10);
      break;

    case 'R':
    case 'W':
      op = getMetricsOp(record);
      record = getNextDigit(record);
      values[op] = strtoul(record, &record, 10);
      break;

    default:
      break;
    }

    record++;
  }

  if (ts != -1 && volId != -1) {
    INFO("ts %lu, vid %u, RDT %u, RDL %u, RDO %u, WRT %u, WRL %u, WRO %u\n",
        ts, volId, values[MetricsOpReadThroughput],
        values[MetricsOpReadLatency], values[MetricsOpReadIOps],
        values[MetricsOpWriteThroughput], values[MetricsOpWriteLatency],
        values[MetricsOpWriteIOps]);

    if (fmhtEntry->curDispatchTs < ts) {
      dispatchEntries(fs, fmhtEntry, fmhtEntry->curOffset + bufOffset, ts); 
    }

    mhtInsert(&fmhtEntry->mht, volId, ts, &values[0]);
  }

  *buffer = record + 1;
}

static void processBuffer(hdfsFS fs, FMHTEntry *fmhtEntry, char *buffer,
              tSize len)
{
  char *initialBuffer = buffer;

  int64_t bufOffset = 0;
  while (buffer < (initialBuffer + len)) {
    processRecord(fs, fmhtEntry, &buffer, bufOffset);
    bufOffset = buffer - initialBuffer;
  }
}

static int64_t readFile(hdfsFS fs, FMHTEntry *fmhtEntry)
{
  char buffer[BUF_SIZE];

  hdfsFile file = hdfsOpenFile(fs, fmhtEntry->filepath, O_RDONLY, 0, 0, 0);
  if (!file) {
    ERROR( "Failed to open %s for reading!\n", fmhtEntry->filepath);
    return errno;
  }

  int i;
  tSize readBytes = 0;
  int64_t totalRead = 0;
  for (i = 0; i < MAX_READ_ITER; ++i) {
    readBytes = hdfsPread(fs, file, fmhtEntry->curOffset, buffer, BUF_SIZE);
    if (readBytes < 0) {
      ERROR("hdfsPread failed for file: %s, offset: %ld", fmhtEntry->filepath,
              fmhtEntry->curOffset);
      break;
    }

    while (readBytes > 0 && buffer[readBytes - 1] != '}')
      readBytes--;

    if (readBytes == 0)
      break;

    buffer[readBytes] = '\0';
    processBuffer(fs, fmhtEntry, buffer, readBytes);
    fmhtEntry->curOffset += readBytes;
    totalRead += readBytes;
  }

  hdfsCloseFile(fs, file);
  return totalRead; 
}

static void processVolumeNamesBuffer(char *buffer)
{
  uint32_t volId;
  char *lineSavePtr, *volSavePtr;
  char *line = strtok_r(buffer, "\n", &lineSavePtr);
  while (line) {
    char *volIdStr = strtok_r(line, ",", &volSavePtr);
    if (volIdStr) {
      volId = strtoul(volIdStr, NULL, 10);
      char *volName = strtok_r(NULL, ",", &volSavePtr);
      vnhtAdd(&vnht, volId, volName);
    }

    line = strtok_r(NULL, "\n", &lineSavePtr);
  }
}

static void readAndCacheVolumeNames(hdfsFS fs, FMHTEntry *fmhtEntry)
{
  char buffer[BUF_SIZE];

  hdfsFile file = hdfsOpenFile(fs, fmhtEntry->filepath, O_RDONLY, 0, 0, 0);
  if (!file) {
    ERROR( "Failed to open %s for reading!\n", fmhtEntry->filepath);
    return;
  }

  tSize readBytes = 0;
  while (true) {
    readBytes = hdfsPread(fs, file, fmhtEntry->curOffset, buffer, BUF_SIZE);
    if (readBytes < 0) {
      ERROR("hdfsPread failed for file: %s, offset: %ld", fmhtEntry->filepath,
              fmhtEntry->curOffset);
      break;
    }

    if (readBytes == 0)
      break;

    buffer[readBytes] = '\0';
    processVolumeNamesBuffer(buffer);
    fmhtEntry->curOffset += readBytes;
  }

  hdfsCloseFile(fs, file);
}

static void readVolumeListFiles(hdfsFS fs, hdfsFileInfo *fileList, int numFiles)
{
  int j;
  bool created = false;
  for (j = 0; j < numFiles; ++j) {
    if (fileList[j].mKind == kObjectKindFile) {
      char *filename = basename(fileList[j].mName);
      if (startsWith(filename, (char *) VOLLIST_FILE_PREFIX,
                  VOLLIST_FILE_PREFIX_LEN)) {
        FMHTEntry *fmhtEntry = fmhtLookupOrInsert(&fmht, fileList[j].mName,
                                  &created);

        if (created || fmhtEntry->curOffset != fileList[j].mSize) {
          readAndCacheVolumeNames(fs, fmhtEntry);
        }
      }
    }
  }
}

static void readMetricsFiles(hdfsFS fs, hdfsFileInfo *fileList, int numFiles)
{
  int j;
  bool created = false;
  for (j = 0; j < numFiles; ++j) {
    if (fileList[j].mKind == kObjectKindFile) {
      char *filename = basename(fileList[j].mName);
      if (startsWith(filename, (char *) METRICS_FILE_PREFIX,
            METRICS_FILE_PREFIX_LEN)) {
        FMHTEntry *fmhtEntry = fmhtLookupOrInsert(&fmht,
                                (const char *)fileList[j].mName, &created);
        if (created) {
          char buf[32];
          ssize_t xattrSize = hdfsGetXattr(fs, fileList[j].mName, XATTR_NAME,
                      &buf[0], 32);
          if (xattrSize == -1 && errno != ENOENT) {
            ERROR("Error when getting xattr %s on %s: %s", XATTR_NAME,
              fileList[j].mName, strerror(errno));
          } else if (xattrSize > 0 && !errno) {
            buf[xattrSize] = '\0';
            fmhtEntry->dispatchedOffset = strtoll((const char *)&buf[0], NULL, 10);
            fmhtEntry->curOffset = fmhtEntry->dispatchedOffset;
          }
        }

        struct tm curTm;
        time_t curTime = time(NULL);

        readFile(fs, fmhtEntry);
        gmtime_r(&curTime, &curTm);

        // NOTE: we check with (size -1 ) instead of just size because the last
        // character is '\n', which will be skipped when setting
        // dispatchedOffset
        if (fmhtEntry->cachedMtime.tm_mday != 0 &&
            fmhtEntry->dispatchedOffset >= (fileList[j].mSize - 1) &&
            fmhtEntry->cachedMtime.tm_mday < curTm.tm_mday &&
            (curTime - fileList[j].mLastMod) > 300) {

          //TODO: remove the entry from FMHT and delte the file
          fmhtRemove(&fmht, (const char *)fileList[j].mName);
          hdfsDelete(fs, fileList[j].mName, false /*recursive*/);
        } else {
          gmtime_r(&fileList[j].mLastMod, &fmhtEntry->cachedMtime);
        }

        curTime = curTime * 1000;
        curTime = curTime - (curTime % WINDOW_SIZE_MS);
        if (fmhtEntry->curDispatchTs && fmhtEntry->curDispatchTs < curTime) {
          dispatchEntries(fs, fmhtEntry, fmhtEntry->curOffset, curTime);
          fmhtEntry->curDispatchTs = 0;
        }
      }
    }
  }
}

static int vm_read(void)
{
  char metricsDir[1024];
  const char *metricsPathPrefix = "/var/mapr/local";

  //Get Hostname
  char hostname[1024];
  hostname[1023] = '\0';
  gethostname(hostname, 1023);
  struct hostent *h;
  h = gethostbyname(hostname);

  //File path prefix
  //const char *filePrefix = "Metrics.log";

  int err = hdfsSetRpcTimeout(30);
  if (err) {
    ERROR( "Failed to set rpc timeout!\n");
    return err;
  }

  hdfsFS fs = hdfsConnect("default", 0);
  if (!fs ) {
    ERROR( "Oops! Failed to connect to hdfs!\n");
    return err;
  }

  sprintf(&metricsDir[0], "%s/%s/%s", metricsPathPrefix, h->h_name,
            METRICS_DIRNAME);

  hdfsFileInfo *info = hdfsGetPathInfo(fs, metricsDir); //audit directory
  if (info) {
    if (info->mKind != kObjectKindDirectory) {
      hdfsFreeFileInfo(info, 1);
      ERROR("Expected directory received file: %s", info->mName);
      return -1;
    }

    int i;
    int numEntries = 0; // Num of mfs instances
    hdfsFileInfo *dirList = hdfsListDirectory(fs, metricsDir, &numEntries);
    if (dirList != NULL) {
      for (i = 0; i < numEntries; ++i) {
        if (dirList[i].mKind == kObjectKindDirectory) {
          int numFiles = 0;
          hdfsFileInfo *fileList = hdfsListDirectory(fs, dirList[i].mName, &numFiles);
          readVolumeListFiles(fs, fileList, numFiles);
          readMetricsFiles(fs, fileList, numFiles);
        }
      }
    } 

    hdfsFreeFileInfo(dirList, numEntries);
    hdfsFreeFileInfo(info, 1);
  } else {
    ERROR("Info is NULL for path %s", metricsDir);
  }

  hdfsDisconnect(fs);
  return 0;
}

void module_register(void)
{
  plugin_register_complex_config("mapr_volmetrics", vm_config);
  plugin_register_init("mapr_volmetrics", vm_init);
  plugin_register_read("mapr_volmetrics", vm_read);
} /* void module_register */
