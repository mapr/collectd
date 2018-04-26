/**
 * collectd - src/mapr_table_metrics.cc
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WitHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILitY or FitNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 **/

#include <cstdint>

extern "C" {

// collectd uses _Bool
#include <stdbool.h>

// collectd boilerplate
#include "collectd.h"
#include "common.h"
#include "plugin.h"

// strcasecmp()
#include <strings.h>

// PATH_MAX
#include <limits.h>

// read(), close(), stat()
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

}

// proto api
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

// input proto
#include "mapr_table_metrics.pb.h"


// output proto
#include "mapr_metrics.pb.h"

// opaque buffer management
#include "mapr_metrics.h"

// hdfsEverything
#include "hdfs.h"

extern "C" {
// from api_support.h:

// populates path based on FID (made of c, i, u)
// its purpose is to be called by clients who can't use FidMsg
extern int hdfsGetPathFromFid2(
  hdfsFS fs,
  uint32_t cid, uint32_t cinum, uint32_t unuq,
  char* path);

// populates secondary index name based on fids for the table and its
// secondary index.
extern int hdfsGetIndexNameFromFids(
  hdfsFS fs,
  uint32_t TableCid, uint32_t TableCinum, uint32_t TableUniq,
  uint32_t IndexCid, uint32_t IndexCinum, uint32_t IndexUniq,
  char* name);
}

// memset()
#include <cstring>

// c++
#include <string>
#include <set>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <atomic>
#include <chrono>
#include <thread>
#include <functional>
#include <vector>

using namespace mapr::fs::tablemetrics;
static constexpr
  std::array<std::pair<int64_t, int64_t>, IndexOfLastTableMetricsBucket + 1>
  kBuckets {{
    {0, bucket0},
    {bucket0, bucket1},
    {bucket1, bucket2},
    {bucket2, bucket3},
    {bucket3, bucket4},
    {bucket4, bucket5},
    {bucket5, bucket6},
    {bucket6, bucket7},
    {bucket7, bucket8},
    {bucket8, bucket9},
    {bucket9, bucket10},
    {bucket10, bucket11},
    {bucket11, bucket12},
    {bucket12, std::numeric_limits<int64_t>::max()}
  }};

//static int log_level = 3;

#define STRINGIFY2(x) #x
#define STRINGIFY(X) STRINGIFY2(X)


// these macros print line numbers (which makes them extra useful)

#define LOG(fmt, ...) plugin_log(LOG_INFO, \
  __FILE__ "::%s@" STRINGIFY(__LINE__) ": " fmt, __FUNCTION__, ## __VA_ARGS__)

#define ERR(fmt, ...) plugin_log(LOG_ERR, \
  "%s@" STRINGIFY(__LINE__) ": " fmt, __PRETTY_FUNCTION__, ## __VA_ARGS__)


struct scopeLogger
{
  const char *name_;
  scopeLogger(const char *name) : name_(name)
  {
    plugin_log(LOG_INFO, ">> %s entered", name_);
  }
  ~scopeLogger()
  {
    plugin_log(LOG_INFO, "<< %s exited", name_);

  }
};

#define CONCAT_(x,y) x##y
#define CONCAT(x,y) CONCAT_(x,y)

#define LOG_SCOPE scopeLogger CONCAT(scope, __COUNTER__)(__FUNCTION__)


namespace cHelpers {


// verbatim copies from mapr_volmetrics.c

#define MAPR_HOSTNAME_FILE "/opt/mapr/hostname"
#define METRICS_PATH_PREFIX "/var/mapr/local"
#define XATTR_NAME "trusted.dispatchedOffset"
#define METRICS_DIRNAME "audit"


extern "C" int
getHostName(char *buf, int len)
{

  int i;
  int fd;
  int err;
  int ret;
  struct stat stbuf;
  char mapr_home[PATH_MAX]; 
  char *env_str;

  env_str = getenv("MAPR_HOME");
  if (env_str) {
    snprintf(mapr_home, PATH_MAX-1, "%s/%s", env_str, "hostname");
    err = stat(mapr_home, &stbuf);
    LOG("MAPR_HOME is set to %s ", mapr_home);
  }

  if (!env_str || (env_str && err)) {
    LOG("MAPR_HOME not found.");
    snprintf(mapr_home, PATH_MAX-1, "%s", MAPR_HOSTNAME_FILE);
    err = stat(mapr_home, &stbuf);
    if (err) {
      return errno;
    }
  }

  fd = open(mapr_home, O_RDONLY);
  if (fd == -1) {
    return errno; 
  }

  ret = read(fd, buf, len-1);
  if (ret < 0) {
    close(fd);
    return errno;
  }

  for (i=ret-1; i>=0; i--) {
    if (buf[i] == '\n') {
      ret = i;
      break;
    }
  }

  buf[ret] = '\0';
  close(fd);
  LOG("MAPR_HOSTNAME : %s:", buf);
  return 0;
}
}

namespace std {
  template<>
  struct less<tmFidMsg> {
    bool operator ()(const tmFidMsg &l, const tmFidMsg &r) const
    {
      const auto lhs = {l.cid(), l.cinum(), l.uniq()};
      const auto rhs = {r.cid(), r.cinum(), r.uniq()};
      return lexicographical_compare(
        lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }
  };
}

struct Fid {
  const tmFidMsg fid_msg_;
  char c_str_[33];

  Fid(tmFidMsg &msg) :
    fid_msg_(msg)
  {
    sprintf(c_str_, "%u.%u.%u", msg.cid(), msg.cinum(), msg.uniq());
  }

  struct blank {};
  Fid(struct blank) :
    c_str_("")
  { }

  bool empty() const
  {
    return c_str_[0] == '\0';
  }

  const char *c_str() const
  {
    return c_str_;
  }

  bool operator <(const Fid &other) const
  {
    return std::less<tmFidMsg>()(fid_msg_, other.fid_msg_);
  }
};


static_assert(std::is_pointer<hdfsFile>::value, "sanity check");

struct cluster {
  template <typename T> struct scopedHandle {
    scopedHandle(T input, std::function<void(T)> closer) :
      handle_(input), closer_(closer)
      {};

    T get() const {
      return handle_;
    }
    
    ~scopedHandle()
    {
      if (handle_ != nullptr) {
        closer_(handle_);
      }
    }

  private:
    T handle_;
    std::function<void(T)> closer_;
  };

  hdfsFS fs_ = nullptr;
  std::string metricsDir_;
  int refreshCount_;

  struct listingFileInfo
  {
    std::string name;
    // from hdfsFileInfo:
    tTime mLastMod = 0;
    tOffset mSize = 0;

    listingFileInfo(const hdfsFileInfo &info) :
      name(info.mName),
      mLastMod(info.mLastMod),
      mSize(info.mSize)
      {}
  };

  std::vector<listingFileInfo> tableMetricsFiles_;

  bool connected()
  {
    return (fs_ != nullptr);
  }
  
  void disconnect()
  {
    LOG("");
    hdfsDisconnect(fs_);
    fs_ = nullptr;
  }

  bool reconnect()
  {
    LOG_SCOPE;
    fs_ = hdfsConnect("default", 0);
    if (fs_ == nullptr) {
      return false;
    }

    auto success = populateHostname();
    if (!success) {
      hdfsDisconnect(fs_);
      fs_ = nullptr;
      return false;
    }

    // ok, we have hostname, let's build the path:
    metricsDir_ = std::string(METRICS_PATH_PREFIX) + '/' 
                + hostname_ + '/' 
                + METRICS_DIRNAME;

    return true;
  }

  bool metricsDirectoryExists()
  {
    hdfsFileInfo *info = hdfsGetPathInfo(fs_, metricsDir_.c_str());
    if (!info) {
      return false;
    }

    auto isDirectory = (info->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(info, 1);

    return isDirectory;
  }

  bool isTableMetricsFile(const hdfsFileInfo &file)
  {
    if (file.mSize <= 0) {
      return false;
    }

    if (file.mKind != kObjectKindFile) {
      return false;
    }

    // /var/mapr/local/atsqa4-104.qa.lab/audit/5660/...
    // <-------------------------------------> this part is exactly the 
    // same as metricsDir_ and it is by construction
    //                                         <--> this part is some string
    // token, yet again, by construction
    // .../5660/TableMetricsAudit.log-2018-01-17-001.pb
    //          <-------------------> this and      <-> this is what we will
    // check:
    
    const char *lastSlashPlusOne = [this](const char *name) {
      const char *pos = nullptr;
      for (auto i = metricsDir_.size(); name[i] != '\0'; ++i) {
        if (name[i] == '/') {
          pos = &name[i + 1];
        }
      }
      return pos;
    }(file.mName);

    if (lastSlashPlusOne == nullptr) {
      // this is impossible
      abort();
    }
    const char *kPrefix = "TableMetricsAudit.log";
    return (strncmp(lastSlashPlusOne, kPrefix, strlen(kPrefix)) == 0);
  }

  bool walkMetricsDirAndEnumFiles()
  {
    tableMetricsFiles_.clear(); 
    int numEntries = 0;
    auto subdirs = hdfsListDirectory(fs_, metricsDir_.c_str(), &numEntries);
    if (subdirs == nullptr) {
      return false;
    }
    for (auto i = 0; i < numEntries; ++i) {
      if (subdirs[i].mKind != kObjectKindDirectory) {
        continue;
      }

      auto numFiles = 0;
      auto files = hdfsListDirectory(fs_, subdirs[i].mName, &numFiles);
      for (auto j = 0; j < numFiles; ++j) {
        if (isTableMetricsFile(files[j])) {
          tableMetricsFiles_.emplace_back(files[j]);
        }
      }
      hdfsFreeFileInfo(files, numFiles);
    }
    hdfsFreeFileInfo(subdirs, numEntries);
    return true;
  }

  int64_t getDispatchedOffset(const std::string &name)
  {
    char buf[32];
    int xattrSize = hdfsGetXattr(fs_, name.c_str(), XATTR_NAME,
                                      buf, sizeof(buf));
    if (xattrSize <= 0) {
      auto temp = errno;
      if (temp == ENOENT) {
        // this is not an error; there is no xattr on the file, it is a file
        // we never dealt with:
      } else {
        LOG("Error when getting xattr " XATTR_NAME " on %s, errno %d",
          name.c_str(), temp);
      }
      errno = temp;
      return 0;
    }

    buf[xattrSize] = '\0';

    auto val = std::strtoul(buf, nullptr, 10);
    LOG("xattr " XATTR_NAME " on %s == %lu", name.c_str(), val);

    return val;
  }

  bool setDispatchedOffset(const std::string &name, int64_t value)
  {
    char buf[24];
    sprintf(buf, "%ld", value);

    LOG("Setting xattr on %s to \"%s\"", name.c_str(), buf);

    auto success = hdfsSetXattr(
      fs_, name.c_str(),
      XATTR_NAME, strlen(XATTR_NAME),
      buf, strlen(buf));

    if (success == 0) {
      LOG("hdfsSetXattr() Success");
      return true;
    }

    auto temp = errno;
    ERROR("Error when setting xattr " XATTR_NAME " on %s, errno %d",
          name.c_str(), temp);
    errno = temp;
    return false;
  }

  scopedHandle<hdfsFile> openForRead(const std::string &name)
  {
    auto handle = hdfsOpenFile(fs_, name.c_str(), O_RDONLY, 0, 0, 0);
    return scopedHandle<hdfsFile>(
      handle, [this](hdfsFile h){hdfsCloseFile(fs_, h);}
    ); 
  }

  int getMapRHostName(char *buf, int len)
  {
    int i;
    int fd;
    int err;
    int ret;
    struct stat stbuf;
    char mapr_home[PATH_MAX]; 
    char *env_str;

    env_str = getenv("MAPR_HOME");
    if (env_str) {
      snprintf(mapr_home, PATH_MAX-1, "%s/%s", env_str, "hostname");
      err = ::stat(mapr_home, &stbuf);
      LOG("MAPR_HOME is set to %s ", mapr_home);
    }

    if (!env_str || (env_str && err)) {
      LOG("MAPR_HOME not found.");
      snprintf(mapr_home, PATH_MAX-1, "%s", MAPR_HOSTNAME_FILE);
      err = stat(mapr_home, &stbuf);
      if (err) {
        return errno;
      }
    }

    fd = open(mapr_home, O_RDONLY);
    if (fd == -1) {
      return errno; 
    }

    ret = read(fd, buf, len-1);
    if (ret < 0) {
      close(fd);
      return errno;
    }

    for (i=ret-1; i>=0; i--) {
      if (buf[i] == '\n') {
        ret = i;
        break;
      }
    }

    buf[ret] = '\0';
    close(fd);
    LOG("MAPR_HOSTNAME : %s", buf);
    return 0;
  }

  bool populateHostname()
  {
    memset(hostname_, 0, sizeof(hostname_));
    auto err = getMapRHostName(hostname_, sizeof(hostname_) - 1);
    if (!err) {
      return true;
    }
    err = gethostname(hostname_, sizeof(hostname_) - 1);
    if (!err) {
      return true;
    }
    // preserve errno across the hdfsDisconnect() call:
    auto tempErrno = errno;
    LOG("Could not get hostname, error %d", tempErrno);
    return false;
  }

  int getPathFromFid(const Fid &fid, char *path)
  {
    auto &msg = fid.fid_msg_;
    return hdfsGetPathFromFid2(fs_, msg.cid(), msg.cinum(), msg.uniq(), path);
  }

  int getIndexNameFromFids(const Fid &table, const Fid &index, char *name)
  {
    auto &tbl = table.fid_msg_;
    auto &idx = index.fid_msg_;
    return hdfsGetIndexNameFromFids(
      fs_,
      tbl.cid(), tbl.cinum(), tbl.uniq(), 
      idx.cid(), idx.cinum(), idx.uniq(), 
      name);
  }

  char hostname_[PATH_MAX];
  static_assert(sizeof(hostname_) == PATH_MAX, "need ARRAYSIZE macro");
};


class tableMetrics {
  const static char kCollectdPluginName[];  // = "mapr_tblmetrics";
  const static char kLogSettingName[];      // = "Log_Config_File";
  const int kCollectdSuccess = 0;

  static int initCallback()
  {
    LOG("entered");
    auto ret = instance()->init();
    LOG("returning %d", ret);
    return ret;
  }

  int init()
  {
    static std::atomic<int> once;
    if ((++once) != 1) {
      // second call to init(), do nothing
      return kCollectdSuccess;
    }
    // do we have any meaningful init? Log maybe?
      return kCollectdSuccess;
  }

  static int readCallback()
  {
    LOG("entered");
    static std::atomic<bool> reentrancy_control;
    auto reentry = reentrancy_control.exchange(true);
    if (reentry) {
      DEBUG("reentry detected, skipping");
      return 0;
    }

    auto ret = instance()->read();
    reentrancy_control = false;
    LOG("returning %d", ret);
    return ret;
  }

  mutable cluster cluster_;

  struct metricsFileData
  {
    int64_t bytesProcessed;
    int64_t storedDispatchedOffset;
    int64_t lastKnownSize = -1;
    struct {
      int read = 0;
      int open = 0;
      int parse = 0;
    } errorsSoFar;
    bool dispatchNow = true;
    static const int kMaxErrors = 5;

    int openRetiresLeft() const
    {
      return kMaxErrors - errorsSoFar.open;
    }

    int readRetiresLeft() const
    {
      return kMaxErrors - errorsSoFar.read;
    }

    int parseRetiresLeft() const
    {
      return kMaxErrors - errorsSoFar.parse;
    }

  };

  // filename -> [position, timestamp]
  std::map<std::string, metricsFileData> knownMetricsFiles_;


  // takes the buffer at data sized bytes bytes. Consumes one record from that
  // buffer. Returns how many bytes were processed.
  int processOneMetricsRecord(const char *data, int bytes)
  {
    // every record in the file has the following format:
    // [00123][protobuf]
    const int lengthBytes = 5;
    if (bytes < lengthBytes) {
      // need more bytes
      // LOG("bytesLeft == %d", bytes);
      return 0;
    }
    char temp[lengthBytes + 1];
    memcpy(temp, data, lengthBytes);
    auto protobufBufferSize = std::atoi(temp);
    if (protobufBufferSize + lengthBytes > bytes) {
      // also need more bytes
      // LOG("%d + %d < %d", protobufBufferSize, lengthBytes, bytes);
      return 0;
    }

    if (protobufBufferSize == 0) {
      // this is how 0-sized msg looks like:
      // 0007a1f0  30 30 33 31 0a 09 08 8c  10 10 24 18 fc 81 08 10  |0031......$.....| <-- 31-byte msg starts here
      // 0007a200  d0 a9 cc fd 90 2c 1a 0b  08 01 10 06 28 be e5 01  |.....,......(...|
      // 0007a210  38 bc 04 00 00 00 00 00  00 00 00 00 00 00 00 00  |8...............| <---- HERE begins a 0-byte message
      // 0007a220  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|      
      LOG("can't parse 0-byte message");
      return -1;
    }

    google::protobuf::io::ArrayInputStream stream(
      &data[lengthBytes], protobufBufferSize);

    MetricsMsg messages;
    auto parsed = messages.ParseFromBoundedZeroCopyStream(
      &stream, protobufBufferSize);
    if (!parsed) {
      LOG("failed parsing message (%d bytes)", protobufBufferSize);
      return -2;
    }
    // LOG("parsed %d bytes", protobufBufferSize + lengthBytes);

    if (!messages.has_table()) {
      LOG("corrupt protobuf file 3 (no table fid)");
      return -2;
    }
    auto fidPb = messages.table();
    if (!fidPb.has_cid() || !fidPb.has_cinum() || !fidPb.has_uniq()) {
      LOG("corrupt protobuf file 4 (fid not fully defined)");
      return -4;
    }

    const Fid tableFid(fidPb);

    const Fid indexFid([messages] {
      if (!messages.has_index()) {
        return Fid(Fid::blank{});
      }

      auto siFid = messages.index();
      if (!siFid.has_cid() || !siFid.has_cinum() || !siFid.has_uniq()) {
        LOG("corrupt protobuf file 7: index fid not fully defined");
        return Fid(Fid::blank{});
      }

      return Fid(siFid);
    }());

    if (!messages.has_timestamp()) {
      LOG("corrupt protobuf file 3 (no timestamp)");
      return -3;
    }
    auto timestamp = messages.timestamp();

    //LOG("%u:%u:%u @%lu", fidPb.cid(), fidPb.cinum(), fidPb.uniq(), timestamp);

    auto &tm = unflushedMetrics_[{tableFid, indexFid}];

    if (tm.timestamp > timestamp) {
      tm.timestamp = timestamp;
    }

    if (messages.has_valuecachehits()) {
      tm.get_valuecache_hits += messages.valuecachehits();
    }
    if (messages.has_valuecachelookups()) {
      tm.get_valuecache_lookups += messages.valuecachelookups();
    }

    for (const auto &perRpc : messages.rpcmetrics()) {
      if (!perRpc.has_op()) {
        LOG("corrupt protobuf file 5 (no rpc type)");
        return -5;
      }

      auto optype = perRpc.op();
      if (!opType_IsValid(optype)) {
        LOG("corrupt protobuf file 6 (unexpected optype = %d)", optype);
        continue;
      }

      auto &metric = tm.perRpc[optype];

      if (perRpc.has_count()) {
        metric.rpcs += perRpc.count();
      }
      if (perRpc.has_readrows()) {
        metric.read_rows += perRpc.readrows();
      }
      if (perRpc.has_resprows()) {
        metric.resp_rows += perRpc.resprows();
      }
      if (perRpc.has_rsizebytes()) {
        metric.read_bytes += perRpc.rsizebytes();
      }
      if (perRpc.has_writerows()) {
        metric.write_rows += perRpc.writerows();
      }
      if (perRpc.has_wsizebytes()) {
        metric.write_bytes += perRpc.wsizebytes();
      }

      for (auto &histoBar : perRpc.latencyhisto()) {
        if (!histoBar.has_index()) {
          continue;
        }

        if (!histoBar.has_count()) {
          continue;
        }

        metric.histo[histoBar.index()] += histoBar.count();
      }
    }

    return protobufBufferSize + lengthBytes;
  }

  static const char *rpcIndexTo_c_str(opType index)
  {
    switch (index)
    {
      case TM_PUT: return "put";
      case TM_CHECKANDPUT: return "check_and_put";
      case TM_UPDATEANDGET: return "update_and_get";
      case TM_APPEND: return "append";
      case TM_INCREMENT: return "inc";
      case TM_GET: return "get";
      case TM_SCAN:  return "scan";
      default: abort();
    }
  }

  enum class metricId
  {
    rpcs,
    write_rows,
    resp_rows,
    read_rows,
    write_bytes,
    read_bytes,
//    get_valuecache_lookups,
//    get_valuecache_hits,
  };

  static const char *to_c_str(metricId metric)
  {
    switch (metric)
    {
      case metricId::rpcs: return "mapr.db.table.rpcs";
      case metricId::write_rows: return "mapr.db.table.write_rows";
      case metricId::resp_rows: return "mapr.db.table.resp_rows";
      case metricId::read_rows: return "mapr.db.table.read_rows";
      case metricId::write_bytes: return "mapr.db.table.write_bytes";
      case metricId::read_bytes: return "mapr.db.table.read_bytes";
      default: abort();
    }
  }

  struct perRpcTableMetricNumbers
  {
    int64_t rpcs = 0;
    int64_t write_rows = 0;
    int64_t resp_rows = 0;
    int64_t read_rows = 0;
    int64_t write_bytes = 0;
    int64_t read_bytes = 0;

    std::array<int64_t, IndexOfLastTableMetricsBucket + 1> histo = {};

    struct enum_per_rpc_metrics_item
    {
      metricId name;
      int64_t value;
    };
    const std::array<enum_per_rpc_metrics_item, 6> enumerate() const
    {
      #define TABLE_METRIX_NAME_AND_VALUE(name) \
        enum_per_rpc_metrics_item{metricId::name, name}
      return { {
        TABLE_METRIX_NAME_AND_VALUE(rpcs),
        TABLE_METRIX_NAME_AND_VALUE(write_rows),
        TABLE_METRIX_NAME_AND_VALUE(resp_rows),
        TABLE_METRIX_NAME_AND_VALUE(read_rows),
        TABLE_METRIX_NAME_AND_VALUE(write_bytes),
        TABLE_METRIX_NAME_AND_VALUE(read_bytes), 
      }};
      #undef TABLE_METRIX_NAME_AND_VALUE
    }

  };

  struct perTable {
    static const int kNumberOfRpcs = mapr::fs::tablemetrics::opType_ARRAYSIZE;
    std::array<perRpcTableMetricNumbers, kNumberOfRpcs> perRpc = {};
    int64_t get_valuecache_hits = 0;
    int64_t get_valuecache_lookups = 0;
    int64_t timestamp = INT64_MAX;
  };

  struct table {
    Fid m_primary;
    Fid m_si;

    bool operator <(const table &other) const
    {
      const auto lhs = {this->m_primary, this->m_si};
      const auto rhs = {other.m_primary, other.m_si};
      return std::lexicographical_compare(
        lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }
  };

  std::map<table, perTable> unflushedMetrics_;

  // we have a map of table -> timestamp [rpc->[, counter]]
  void processOneMetricsFile(const std::string& name, metricsFileData& details)
  {
    if (details.openRetiresLeft() <= 0) {
      // we already gave up on this file, don't touch it again
      LOG("%s: details.openRetiresLeft() == %d",
        name.c_str(), details.openRetiresLeft());
      details.dispatchNow = false;
      return;
    }
    LOG("Processing %s", name.c_str());

    auto h = cluster_.openForRead(name);
    if (!h.get()) {
      auto err = errno;
      ++details.errorsSoFar.open;
      LOG("can't open %s, errno %d, will retry %d more times",
        name.c_str(), err, details.openRetiresLeft());
      errno = err;
      return;
    }

    std::array<char, 256 * 1024> buffer;
    for (;;) {
      const auto pbBytesRead = hdfsPread(
        cluster_.fs_, h.get(),
        details.bytesProcessed,
        buffer.data(), buffer.size());
      if (pbBytesRead == 0) {
        break;
      }

      LOG("read %d bytes from %s", pbBytesRead, name.c_str());

      if (pbBytesRead == -1) {
        // always an error;
        if ((errno == EINTR) && (details.readRetiresLeft() > 0)) {
          // this is a retriable error,
          NOTICE("hdfsRead(%s) failed with EINTR", name.c_str());
          ++details.errorsSoFar.read;
          // consider
          //  std::this_thread::sleep_for(std::chrono::milliseconds(20));
          // and/or
          //  continue;
          // we will come back.
        } else {
          // this is not retriable
          details.dispatchNow = false;
        }
        return;
      }
      
      if (pbBytesRead < 0) {
        // impossible case, hdfsPread() doesn't return this:
        LOG("BUGBUG: hdfsPread() returned %d, unexpected",
          pbBytesRead);
        abort();
      }

      // parse the records, one after another:
      auto bytesParsedSoFar = decltype(pbBytesRead){0};
      while (bytesParsedSoFar < pbBytesRead) {
        // LOG("parsed %d bytes so far", bytesParsedSoFar);
        auto bytesParsedInOneTake = processOneMetricsRecord(
          &buffer[bytesParsedSoFar],
          pbBytesRead - bytesParsedSoFar);
        if (bytesParsedInOneTake == 0) {
          break;
        }
        if (bytesParsedInOneTake < 0) {
          LOG("can't parse [%s] at %lu (of %lu)",
            name.c_str(), details.bytesProcessed + bytesParsedSoFar,
            details.lastKnownSize);
          ++details.errorsSoFar.parse;
          if (details.parseRetiresLeft() < 0) {
            details.dispatchNow = false;
            details.bytesProcessed = details.lastKnownSize;
            return;
          } else {
            break;
          }
        }

        // if we parsed anything, parse error counter goes to 0:
        details.errorsSoFar.parse = 0;
        bytesParsedSoFar += bytesParsedInOneTake;
      }

      details.bytesProcessed += bytesParsedSoFar;
    } // for(;;)
  }


  void processOneEnumeratedFile(const cluster::listingFileInfo &file)
  {
    auto found = knownMetricsFiles_.find(file.name);

    if (found == knownMetricsFiles_.end()) {
      // new file. What is the xattr value in there?
      auto it = knownMetricsFiles_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(std::move(file.name)),
        std::forward_as_tuple()); // metricsFileData is default-constructed

      auto &name = it.first->first;
      auto &data = it.first->second;
      data.storedDispatchedOffset = cluster_.getDispatchedOffset(name);
      data.bytesProcessed = data.storedDispatchedOffset;
      data.lastKnownSize = file.mSize;
      // if we dispatched it through the end, don't dispatch again
      data.dispatchNow = (data.bytesProcessed < data.lastKnownSize);
      return;
    }

    // previously existing file. We have already read and/or updated
    // dispatchedOffset.

    // Has this file grown?
    if (found->second.lastKnownSize < file.mSize) {
      // yes.
      found->second.lastKnownSize = file.mSize;
      found->second.dispatchNow = true;
      return;
    }
    // no. No need to do anything.
  }

  void reset()
  {
    for (auto &t : unflushedMetrics_) {
      auto &perTableData = t.second;
      if (perTableData.timestamp == INT64_MAX) {
        continue;
      }
      perTableData.timestamp = INT64_MAX;
      perTableData.perRpc = {};
    }
  }

  void addEverything(
    Metric *m,
    const Fid &table, const Fid &index, opType op, int64_t timestamp) const
  {
    m->set_time(timestamp);
    addTableTag(table, m);
    addIndexTag(table, index, m);
    addRpcTag(op, m);
  }

  void addTableTag(const Fid &key, Metric *m) const
  {
    auto tag_fid = m->add_tags();
    tag_fid->set_name("table_fid");
    tag_fid->set_value(key.c_str());

    char buffer[PATH_MAX];
    int fid_error = cluster_.getPathFromFid(key, buffer);

    if (fid_error != 0) {
      ERROR("getPathFromFid(%s) returned %d", key.c_str(), fid_error);
    } else {
      auto tag_path = m->add_tags();
      tag_path->set_name("table_path");
      tag_path->set_value(buffer);
    }
  }

  void addIndexTag(const Fid &table, const Fid &index, Metric *m) const
  {
    if (index.empty()) {
      return;
    }
    auto tag_index_fid = m->add_tags();
    tag_index_fid->set_name("index_fid");
    tag_index_fid->set_value(index.c_str());

    char buffer[PATH_MAX];
    int fid_error = cluster_.getIndexNameFromFids(table, index, buffer);
    if (fid_error != 0) {
      ERROR("getIndexNameFromFids(%s, %s) returned %d",
        table.c_str(), index.c_str(), fid_error);
    } else {
      auto tag_path = m->add_tags();
      tag_path->set_name("index");
      tag_path->set_value(buffer);
    }
  }

  void addRpcTag(opType op, Metric *m) const
  {
    auto tag_rpc = m->add_tags();
    tag_rpc->set_name("rpc_type");
    tag_rpc->set_value(rpcIndexTo_c_str(op));
  }

  // max is the max count of non-histogram metrics to produce:
  void flush2() const
  {
    Metrics message;
    for (auto &it : unflushedMetrics_) {
      const auto &key = it.first;
      const auto &table = key.m_primary;
      const auto &index = key.m_si;
      const auto &perTableData = it.second;
      if (perTableData.timestamp == INT64_MAX) {
          continue;
      }
      auto i = 0;
      for (const auto &rpc : perTableData.perRpc) {
        if (!opType_IsValid(i)) {
          continue;
        }
        auto op = static_cast<opType>(i);
        ++i;

        if (rpc.rpcs == 0) {
          continue;
        }

        // for each RPC we have a bunch of metrics
        for (const auto &metric : rpc.enumerate()) {
          if (metric.value == 0) {
            continue;
          }

          auto m = message.add_metrics();

          m->mutable_value()->set_number(metric.value);
          m->set_name(to_c_str(metric.name));
          addEverything(m, table, index, op, perTableData.timestamp);
        }

        // and a histogram. Histogram has several buckets:
        static_assert(kBuckets.size() == decltype(rpc.histo){}.size(), "");
        static_assert(kBuckets.size() ==
          IndexOfLastTableMetricsBucket + 1, "");

        auto mHisto = message.add_metrics();
        auto histo = mHisto->mutable_value();

        // should rewrite in form of inner_product
        for (size_t bkt = 0; bkt < kBuckets.size(); ++bkt) {
          auto countInBucket = rpc.histo[bkt];
          if (countInBucket == 0) {
            continue;
          }
          auto bucket = histo->add_buckets();
          bucket->set_start(kBuckets[bkt].first);
          bucket->set_end(kBuckets[bkt].second);
          bucket->set_number(countInBucket);
        }

        mHisto->set_name("mapr.db.table.latency");
        addEverything(mHisto, table, index, op, perTableData.timestamp);
      }
    }

    auto cb = message.ByteSize();
    if (cb == 0) {
      // no messages;
      return;
    }
    auto buffer = AllocateMetricsPointer(cb);
    namespace pb = google::protobuf::io;

    auto bMustSucceed = message.SerializeToArray(buffer->data, cb);
    if (!bMustSucceed) {
      ERR("Failed to serialize the opaque message for writer, %d bytes.", cb);
      return;
    }

    value_list_t vl = { 0 };
    value_t dummy;
    dummy.counter = 12345;
    vl.values = &dummy;
    strcpy(vl.plugin, "mapr.db.table");
    vl.interval = 0;
    vl.time = cdtime();
    vl.values_len = 1;
    EncodeMetricsPointer(&vl, buffer);
    DEBUG(
      "Encoded opaque buffer: ptr 0x%p, type_instance %s, type %s, size %d",
      buffer, vl.type_instance, vl.type, cb);
    plugin_dispatch_values(&vl);

  }


  int read()
  {
    // let's make sure we have a good cluster:
    if (!cluster_.connected()) {
      LOG("connected() == false");
      cluster_.reconnect();
      if (cluster_.connected()) {
        LOG("connected() now true");
      }
    }
    if (!cluster_.connected()) {
      LOG("connected() still false");
      return -1;
    }

    // I have a handle to the cluster; let's check if there is a metrics
    // directory. If there is not, we will retain the connection:
    if (!cluster_.metricsDirectoryExists()) {
      LOG("metricsDirectoryExists() == false");
      return -1;
    }

    // traverse the metrics directory and build the list of metrics files
    if (!cluster_.walkMetricsDirAndEnumFiles()) {
      return -1;
    };

    for (const auto &file : cluster_.tableMetricsFiles_) {
      processOneEnumeratedFile(file);
    }

    // at this point all files are in knownMetricsFiles_
    for (auto &it : knownMetricsFiles_) {
      const auto &name = it.first;
      auto &details = it.second;
      if (!details.dispatchNow) {
        // LOG("details.dispatchNow == false, next");
        continue;
      }
      processOneMetricsFile(name, details);
      cluster_.setDispatchedOffset(name, details.bytesProcessed);
      details.storedDispatchedOffset = details.bytesProcessed;
    }
    flush2();

    reset();

    return 0;
  }

  static tableMetrics *instance()
  {
    static tableMetrics s_metrics;
    return &s_metrics;
  }

  std::string log_conf_;

  // config callback happens before anything else. it might tell us whether
  // we have a log file
  static int configCallback(oconfig_item_t *item)
  {
    return instance()->addConfig(item);
  }

  int addConfig(oconfig_item_t *config_item)
  {
    for (auto index = 0; index < config_item->children_num; ++index) {
      auto c = &config_item->children[index];
      if (strcasecmp(c->key, "Log_Config_File") != 0) {
        continue;
      }

      if (c->values_num != 1) {
        ERROR(
          "%s plugin: Config.Log set incorrectly. "
          "values_num != 1", kCollectdPluginName);
        continue;
      }

      if (OCONFIG_TYPE_STRING != c->values[0].type) {
        ERROR(
          "%s plugin: Config.Log set incorrectly. "
          "OCONFIG_TYPE_STRING != c->values[0].type",
          kCollectdPluginName);
        continue;
      }

      INFO("%s: Logging can be configured through %s",
      kCollectdPluginName,
        c->values[0].value.string);
      log_conf_ = c->values[0].value.string;
      break;
    }
    return 0;
  }

public:
  static void registerPlugin()
  {
    ::plugin_register_complex_config(kCollectdPluginName, configCallback);
    ::plugin_register_init(kCollectdPluginName, initCallback);
    ::plugin_register_read(kCollectdPluginName, readCallback);
  }
};

const char tableMetrics::kCollectdPluginName[] = "mapr_tblmetrics";
const char tableMetrics::kLogSettingName[] = "Log_Config_File";

extern "C"
void module_register(void)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  tableMetrics::registerPlugin();
} /* void module_register */
