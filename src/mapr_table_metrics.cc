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

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <string>
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
    {bucket12, bucket12 + 1}
  }};

//static int log_level = 3;

// we try to do our job while keeping the workign set small. If we digested
// more than kIntakeThreshold bytes in one instance of plugin execution, we
// try to finish up the work and get out.
constexpr int kIntakeThreshold = 1024 * 1024;

// how many bytes to read from metrics file in one shot. buffer is allocated
// on the stack so it should nto be stupid large:
constexpr int kMetricsFileReadSize = 256 * 1024;

// if collectd wasn't running for a while, we start with a backlog. THere are
// two conflicting goals: 1) reasonable workign set and 2) forward progress.
// We should ingest metrics faster then they are produced. So although we will
// honor kIntakeThreshold, we will never process less than
// kBacklogMinimumProgressSeconds seconds of metrics data during one read
// callback
constexpr int kBacklogProgressSeconds = 120;

// plugin maintains the working set of known pairs (table, index) and their
// metrics in a map. If table or index go away forever, stale entry will just
// sit there in the map until plugin exits, producing unbounded memory usage --
// unless we do something about it.
constexpr int kMaxMetricsAgeMinutes = 10;

#define STRINGIFY2(x) #x
#define STRINGIFY(X) STRINGIFY2(X)


// these macros print line numbers (which makes them extra useful)

#define LOG(fmt, ...) plugin_log(LOG_INFO, \
  __FILE__ "::%s@" STRINGIFY(__LINE__) ": " fmt, __FUNCTION__, ## __VA_ARGS__)

#define ERR(fmt, ...) plugin_log(LOG_ERR, \
  "%s@" STRINGIFY(__LINE__) ": " fmt, __PRETTY_FUNCTION__, ## __VA_ARGS__)


// verbatim copies from mapr_volmetrics.c

#define MAPR_HOSTNAME_FILE "/opt/mapr/hostname"
#define METRICS_PATH_PREFIX "/var/mapr/local"
#define XATTR_NAME "trusted.dispatchedOffset"
#define METRICS_DIRNAME "audit"


namespace std {
  template<> struct less<tmFidMsg> {
    bool operator ()(const tmFidMsg &l, const tmFidMsg &r) const
    {
      const auto lhs = {l.cid(), l.cinum(), l.uniq()};
      const auto rhs = {r.cid(), r.cinum(), r.uniq()};
      return lexicographical_compare(
        lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }
  };
}

namespace {
struct Fid {
  const tmFidMsg fid_msg_;
  char c_str_[33];

  Fid(const tmFidMsg &msg) :
    fid_msg_(msg)
  {
    sprintf(c_str_, "%u.%u.%u", msg.cid(), msg.cinum(), msg.uniq());
  }

  struct blank {};
  Fid(struct blank)
  {
    c_str_[0] = '\0';
  }

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

struct ListingFileInfo {
  std::string name;
  // from hdfsFileInfo:
  tTime last_mod = 0;
  tOffset size = 0;

  ListingFileInfo(const hdfsFileInfo &info)
      : name(info.mName),
        last_mod(info.mLastMod),
        size(info.mSize) {}
};

class LocalFile {
  hdfsFS fs_;
  hdfsFile file_;
  const std::string &name_;

 public:
  LocalFile(hdfsFS cluster, hdfsFile file, const std::string &name) : 
    fs_(cluster), file_(file), name_(name) {}

  LocalFile(LocalFile&& other) :
    fs_(other.fs_), file_(other.file_), name_(other.name_)
  {
    other.file_ = nullptr;
  }

  bool Valid() { return file_ != nullptr; }
  ~LocalFile()
  {
    if (file_ != nullptr) {
      hdfsCloseFile(fs_, file_);
    }
  }
  const char *c_str()
  {
    return name_.c_str();
  }

  template <typename T> int32_t Read(int64_t offset, T *buffer)
  {
    assert(file_ != nullptr);
    return hdfsPread(fs_, file_, offset, buffer->data(), buffer->size());
  }
};


class MaprCluster {
 private:
  hdfsFS fs_ = nullptr;
  std::string metrics_dir_;
  std::vector<ListingFileInfo> enum_results_;
  char hostname_[PATH_MAX];

  bool Connected() const { return (fs_ != nullptr); }
  void Disconnect() { hdfsDisconnect(fs_); fs_ = nullptr; }
  bool Reconnect();
  bool PopulateHostname();

  bool MetricsDirectoryExists();
  bool IsTableMetricsFile(const hdfsFileInfo &file);

 public:
  const char *Hostname() { return hostname_; }

  const decltype(enum_results_) &GetMetricsFiles() { return enum_results_; }
  bool EnumerateMetricsFiles();

  int GetPathFromFid(const Fid &fid, char *path);
  int GetIndexNameFromFids(const Fid &table, const Fid &index, char *name);

  int64_t GetDispatchedOffset(const std::string &name);
  bool SetDispatchedOffset(const std::string &name, int64_t value);

  LocalFile OpenFileForRead(const std::string &name)
  {
    auto handle = hdfsOpenFile(fs_, name.c_str(), O_RDONLY, 0, 0, 0);
    return LocalFile(fs_, handle, name);
  }
};
} // anonymous namespace

int MaprCluster::GetPathFromFid(const Fid &fid, char *path)
{
  auto &msg = fid.fid_msg_;
  return hdfsGetPathFromFid2(fs_, msg.cid(), msg.cinum(), msg.uniq(), path);
}

int MaprCluster::GetIndexNameFromFids(
  const Fid &table, const Fid &index, char *name)
{
  auto &tbl = table.fid_msg_;
  auto &idx = index.fid_msg_;
  return hdfsGetIndexNameFromFids(
    fs_,
    tbl.cid(), tbl.cinum(), tbl.uniq(), 
    idx.cid(), idx.cinum(), idx.uniq(), 
    name);
}

bool MaprCluster::MetricsDirectoryExists()
{
  hdfsFileInfo *info = hdfsGetPathInfo(fs_, metrics_dir_.c_str());
  if (!info) {
    return false;
  }

  auto isDirectory = (info->mKind == kObjectKindDirectory);
  hdfsFreeFileInfo(info, 1);

  return isDirectory;
}

int64_t MaprCluster::GetDispatchedOffset(const std::string &name)
{
  char buf[32];
  int xattr_size = hdfsGetXattr(fs_, name.c_str(), XATTR_NAME,
                                    buf, sizeof(buf));
  if (xattr_size <= 0) {
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

  buf[xattr_size] = '\0';

  auto val = std::strtoul(buf, nullptr, 10);
  LOG("xattr " XATTR_NAME " on %s == %lu", name.c_str(), val);

  return val;
}

bool MaprCluster::SetDispatchedOffset(const std::string &name, int64_t value)
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


bool MaprCluster::Reconnect()
{
  fs_ = hdfsConnect("default", 0);
  if (fs_ == nullptr) {
    return false;
  }

  auto success = PopulateHostname();
  if (!success) {
    hdfsDisconnect(fs_);
    fs_ = nullptr;
    return false;
  }

  // ok, we have hostname, let's build the path:
  metrics_dir_ = std::string(METRICS_PATH_PREFIX) + '/' 
              + hostname_ + '/' 
              + METRICS_DIRNAME;
  return true;
}

static int GetMapRHostName(char *buf, int len)
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


bool MaprCluster::PopulateHostname()
{
  memset(hostname_, 0, sizeof(hostname_));
  auto err = GetMapRHostName(hostname_, sizeof(hostname_) - 1);
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

bool MaprCluster::EnumerateMetricsFiles()
{
  // let's make sure we have a good cluster:
  if (!Connected()) {
    LOG("Connected() == false");
    Reconnect();
    if (!Connected()) {
      LOG("connected() still false");
      return false;
    }
    LOG("connected() now true");
  }

  // I have a handle to the cluster; let's check if there is a metrics
  // directory. If there is not, we will retain the connection:
  if (!MetricsDirectoryExists()) {
    LOG("MetricsDirectoryExists() == false");
    return false;
  }

  enum_results_.clear(); 
  int num_subdirs = 0;
  auto subdirs = hdfsListDirectory(fs_, metrics_dir_.c_str(), &num_subdirs);
  if (subdirs == nullptr) {
    return false;
  }
  for (auto i = 0; i < num_subdirs; ++i) {
    if (subdirs[i].mKind != kObjectKindDirectory) {
      continue;
    }

    auto num_files = 0;
    auto files = hdfsListDirectory(fs_, subdirs[i].mName, &num_files);
    for (auto j = 0; j < num_files; ++j) {
      if (IsTableMetricsFile(files[j])) {
        enum_results_.emplace_back(files[j]);
      }
    }
    hdfsFreeFileInfo(files, num_files);
  }
  hdfsFreeFileInfo(subdirs, num_subdirs);
  return true;
}

bool MaprCluster::IsTableMetricsFile(const hdfsFileInfo &file)
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
  // it is VERY IMPORTANT that the names enable natural sort order, that is,
  // when alphabetically sorted, earlier files are before later files. We rely
  // on this.

  const char *last_slash_plus_one = [this](const char *name) {
    const char *pos = nullptr;
    for (auto i = metrics_dir_.size(); name[i] != '\0'; ++i) {
      if (name[i] == '/') {
        pos = &name[i + 1];
      }
    }
    return pos;
  }(file.mName);

  if (last_slash_plus_one == nullptr) {
    // this is impossible
    abort();
  }
  const char *kPrefix = "TableMetricsAudit.log";
  return (strncmp(last_slash_plus_one, kPrefix, strlen(kPrefix)) == 0);
}


class TableMetricsPlugin {
  const static char kCollectdPluginName[];  // = "mapr_tblmetrics";
  const static char kLogSettingName[];      // = "Log_Config_File";
  const int kCollectdSuccess = 0;

  static int InitCallback()
  {
    LOG("entered");
    auto ret = instance()->Init();
    LOG("returning %d", ret);
    return ret;
  }

  int Init()
  {
    static std::atomic<int> once;
    if ((++once) != 1) {
      // second call to init(), do nothing
      return kCollectdSuccess;
    }
    // do we have any meaningful init? Log maybe?
    return kCollectdSuccess;
  }

  static int ReadCallback()
  {
    LOG("entered");
    static std::atomic<bool> reentrancy_control;
    auto reentry = reentrancy_control.exchange(true);
    if (reentry) {
      DEBUG("reentry detected, skipping");
      return 0;
    }

    auto ret = instance()->Read();
    reentrancy_control = false;
    LOG("returning %d", ret);
    return ret;
  }

  mutable MaprCluster cluster_;

  struct MetricsFileData
  {
    int64_t bytes_processed;
    int64_t stored_dispatched_offset;
    int64_t last_known_size = -1;
    struct {
      int read = 0;
      int open = 0;
      int parse = 0;
    } errors_so_far;
    bool dispatch_now = true;
    static const int kMaxErrors = 5;

    int OpenRetiresLeft() const
    {
      return kMaxErrors - errors_so_far.open;
    }

    int ReadRetiresLeft() const
    {
      return kMaxErrors - errors_so_far.read;
    }

    int ParseRetiresLeft() const
    {
      return kMaxErrors - errors_so_far.parse;
    }

  };

  // filename -> [position, timestamp]
  std::map<std::string, MetricsFileData> known_metrics_files_;
  MetricsMsg scratch_;

  struct ParseResult {
    int err;
    int bytes;
  };

  // takes the buffer at data sized bytes bytes. Consumes one record from that
  // buffer. Returns how many bytes were processed and/or error.
  ParseResult ProcessOneMetricsRecord(const char *data, int bytes)
  {
    // every record in the file has the following format:
    // [00123][protobuf]
    const int kLengthBytes = 5;
    if (bytes < kLengthBytes) {
      // need more bytes
      // LOG("bytesLeft == %d", bytes);
      return ParseResult{EOF, 0};
    }
    char temp[kLengthBytes + 1];
    memcpy(temp, data, kLengthBytes);
    temp[kLengthBytes] = '\0';
  
    auto protobuf_bytes = std::atoi(temp);
    if (protobuf_bytes + kLengthBytes > bytes) {
      // also need more bytes
      // LOG("%d + %d < %d", protobufBufferSize, lengthBytes, bytes);
      return ParseResult{EOF, 0};
    }

    if (protobuf_bytes == 0) {
      // this is how 0-sized msg looks like:
      // 0007a1f0  30 30 33 31 0a 09 08 8c  10 10 24 18 fc 81 08 10  |0031......$.....| <-- 31-byte msg starts here
      // 0007a200  d0 a9 cc fd 90 2c 1a 0b  08 01 10 06 28 be e5 01  |.....,......(...|
      // 0007a210  38 bc 04 00 00 00 00 00  00 00 00 00 00 00 00 00  |8...............| <-- 0-byte message begins at 7a213
      // 0007a220  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|      
      LOG("can't parse 0-byte message");
      return ParseResult{ENOMSG, 0};
    }

    assert(protobuf_bytes > 0);

    auto parsed = scratch_.ParseFromArray(&data[kLengthBytes], protobuf_bytes);
    if (!parsed) {
      LOG("failed parsing message (%d bytes)", protobuf_bytes);
      return ParseResult{ENOMSG, 0};
    }
    // LOG("parsed %d bytes", protobufBytes + lengthBytes);
    if (!scratch_.has_timestamp()) {
      LOG("corrupt protobuf file 3 (no timestamp)");
      return ParseResult{EPROTO, 0};
    }

    if (early_finish.ShouldStop(protobuf_bytes, scratch_.timestamp())) {
      // too much data processed during this callback.
      return ParseResult{EFBIG, 0};
    }

    if (!scratch_.has_table()) {
      LOG("corrupt protobuf file 3 (no table fid)");
      return ParseResult{EPROTO, 0};
    }
    auto fid_msg = scratch_.table();
    if (!fid_msg.has_cid() || !fid_msg.has_cinum() || !fid_msg.has_uniq()) {
      LOG("corrupt protobuf file 4 (fid not fully defined)");
      return ParseResult{EPROTO, 0};
    }

    const Fid table_fid(fid_msg);

    const Fid index_fid([this] {
      if (!scratch_.has_index()) {
        return Fid(Fid::blank{});
      }

      const auto &siFid = scratch_.index();
      if (!siFid.has_cid() || !siFid.has_cinum() || !siFid.has_uniq()) {
        LOG("corrupt protobuf file 7: index fid not fully defined");
        LOG("table %u.%u.%u index %d/%d/%d; %u.%u.%u",
          scratch_.table().cid(), scratch_.table().cinum(),
          scratch_.table().uniq(), scratch_.index().has_cid(),
          scratch_.index().has_cinum(), scratch_.index().has_uniq(),
          scratch_.index().cid(), scratch_.index().cinum(),
          scratch_.index().uniq());
        return Fid(Fid::blank{});
      }

      return Fid(siFid);
    }());


    //LOG("%u:%u:%u @%lu", fidPb.cid(), fidPb.cinum(), fidPb.uniq(), timestamp);

    auto &backlog = unflushed_metrics_[{table_fid, index_fid}];
    const auto high_water_mark = std::min(
      backlog.high_water_mark, global_high_water_mark_);
    const auto timestamp = (scratch_.timestamp() > high_water_mark)
      ? scratch_.timestamp()
      : high_water_mark + 1;

    auto pos = std::partition_point(
      backlog.timeline.begin(), backlog.timeline.end(),
      [timestamp](Timepoint &m) {
        return m.timestamp_ < timestamp;
      });

    // two possible options now. We are inserting a data point with a new
    // timestamp, or updating an existing one. For update, partition_point
    // will never return ::end() for update since equal timestamps do not
    // compare less. Therefore if it is end, it is insert
    auto &tm =
      ((pos != backlog.timeline.end()) && (pos->timestamp_ == timestamp))
      ? *pos
      : *backlog.timeline.emplace(pos, timestamp);

    for (const auto &perRpc : scratch_.rpcmetrics()) {
      if (!perRpc.has_op()) {
        LOG("corrupt protobuf file 5 (no rpc type)");
        return ParseResult{EPROTO, 0};
      }

      auto optype = perRpc.op();
      if (!opType_IsValid(optype)) {
        LOG("corrupt protobuf file 6 (unexpected optype = %d)", optype);
        return ParseResult{EPROTO, 0};
        continue;
      }

      auto &metric = tm.numbers_[optype];

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
      if (perRpc.has_valuecachelookups()) {
        metric.value_cache_lookups += perRpc.valuecachelookups();
      }
      if (perRpc.has_valuecachehits()) {
        metric.value_cache_hits += perRpc.valuecachehits();
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

    return ParseResult{0, protobuf_bytes + kLengthBytes};
  }

  static const char *to_c_str(opType index)
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

  enum class MetricId
  {
    rpcs,
    write_rows,
    resp_rows,
    read_rows,
    write_bytes,
    read_bytes,
    value_cache_lookups,
    value_cache_hits,
  };

  static const char *to_c_str(MetricId metric)
  {
    switch (metric)
    {
      case MetricId::rpcs: return "mapr.db.table.rpcs";
      case MetricId::write_rows: return "mapr.db.table.write_rows";
      case MetricId::resp_rows: return "mapr.db.table.resp_rows";
      case MetricId::read_rows: return "mapr.db.table.read_rows";
      case MetricId::write_bytes: return "mapr.db.table.write_bytes";
      case MetricId::read_bytes: return "mapr.db.table.read_bytes";
      case MetricId::value_cache_lookups:
        return "mapr.db.table.value_cache_lookups";
      case MetricId::value_cache_hits:
        return "mapr.db.table.value_cache_hits";
      default: abort();
    }
  }

  struct PerRpcTableMetricNumbers
  {
    int64_t rpcs = 0;
    int64_t write_rows = 0;
    int64_t resp_rows = 0;
    int64_t read_rows = 0;
    int64_t write_bytes = 0;
    int64_t read_bytes = 0;
    int64_t value_cache_lookups = 0;
    int64_t value_cache_hits = 0;

    std::array<int64_t, IndexOfLastTableMetricsBucket + 1> histo = {};

    struct EnumPerRpcMetricsItem
    {
      MetricId name;
      int64_t value;
    };
    const std::array<EnumPerRpcMetricsItem, 8> enumerate() const
    {
      #define TABLE_METRIX_NAME_AND_VALUE(name) \
        EnumPerRpcMetricsItem{MetricId::name, name}
      return {{
        TABLE_METRIX_NAME_AND_VALUE(rpcs),
        TABLE_METRIX_NAME_AND_VALUE(write_rows),
        TABLE_METRIX_NAME_AND_VALUE(resp_rows),
        TABLE_METRIX_NAME_AND_VALUE(read_rows),
        TABLE_METRIX_NAME_AND_VALUE(write_bytes),
        TABLE_METRIX_NAME_AND_VALUE(read_bytes), 
        TABLE_METRIX_NAME_AND_VALUE(value_cache_lookups),
        TABLE_METRIX_NAME_AND_VALUE(value_cache_hits),
      }};
      #undef TABLE_METRIX_NAME_AND_VALUE
    }

  };

  struct Timepoint {
    Timepoint(int64_t timestamp)
        : timestamp_(timestamp) {}
    std::array<PerRpcTableMetricNumbers, opType_ARRAYSIZE> numbers_ = {};
    int64_t timestamp_;
  };

  struct History {
    int64_t high_water_mark = 0;
    std::chrono::time_point<std::chrono::steady_clock> high_water_time;
    std::vector<Timepoint> timeline;
  };

  struct Table {
    Fid primary_;
    Fid si_;

    bool operator <(const Table &other) const
    {
      const auto lhs = {this->primary_, this->si_};
      const auto rhs = {other.primary_, other.si_};
      return std::lexicographical_compare(
        lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }
  };

  int64_t global_high_water_mark_ = 0;
  std::map<Table, History> unflushed_metrics_;

  // we have a map of table -> timestamp [rpc->[, counter]]
  void ProcessOneMetricsFile(const std::string& name, MetricsFileData* details)
  {
    if (details->OpenRetiresLeft() <= 0) {
      // we already gave up on this file, don't touch it again
      LOG("%s: details.openRetiresLeft() == %d",
        name.c_str(), details->OpenRetiresLeft());
      details->dispatch_now = false;
      return;
    }
    LOG("Processing %s", name.c_str());

    auto file = cluster_.OpenFileForRead(name);
    if (!file.Valid()) {
      auto err = errno;
      ++details->errors_so_far.open;
      LOG("can't open %s, errno %d, will retry %d more times",
        name.c_str(), err, details->OpenRetiresLeft());
      errno = err;
      return;
    }

    for (;;) {
      bool proceed = KeepParsingOneMetricsFile(file, details);
      if (!proceed) {
        break;
      }
    }
  }

  // returns whether to contunue parsing same file.
  bool KeepParsingOneMetricsFile(LocalFile &file, MetricsFileData *details)
  {
    std::array<char, kMetricsFileReadSize> buffer;
    const auto bytes_read = file.Read(details->bytes_processed, &buffer);
    if (bytes_read == 0) {
      return false;
    }

    LOG("read %d bytes from %s", bytes_read, file.c_str());

    if (bytes_read == -1) {
      // always an error;
      if ((errno == EINTR) && (details->ReadRetiresLeft() > 0)) {
        // this is a retriable error,
        NOTICE("hdfsRead(%s) failed with EINTR", file.c_str());
        ++details->errors_so_far.read;
        // consider
        //  std::this_thread::sleep_for(std::chrono::milliseconds(20));
        // and/or continue. We will come back.
      } else {
        // this is not retriable
        details->dispatch_now = false;
      }
      return false;
    }
    
    if (bytes_read < 0) {
      // impossible case, hdfsPread() doesn't return this:
      LOG("BUGBUG: hdfsPread() returned %d, unexpected", bytes_read);
      abort();
    }

    // parse the records, one after another:
    auto bytes_parsed_so_far = decltype(bytes_read){0};
    while (bytes_parsed_so_far < bytes_read) {
      const auto result = ProcessOneMetricsRecord(
        &buffer[bytes_parsed_so_far], bytes_read - bytes_parsed_so_far);

      if (result.err == 0) {
        assert(result.bytes > 0);
        // if we parsed anything, parse error counter goes to 0:
        details->errors_so_far.parse = 0;
        bytes_parsed_so_far += result.bytes;
        details->bytes_processed += result.bytes;
        continue;
      }

      if (result.err == EOF) {
          break;
      }

      if (result.err == EFBIG) {
        // we are using way too much memory, let's come back to this file
        // on next take
        return false;
      }

      LOG("can't parse [%s] at %lu (of %lu), error %d", file.c_str(),
        details->bytes_processed, details->last_known_size, result.err);

      ++details->errors_so_far.parse;
      if (details->ParseRetiresLeft() >= 0) {
        break;
      }

      details->dispatch_now = false;
      details->bytes_processed = details->last_known_size;
      return false;
    }

    return bytes_read == buffer.size();
  }

  void ProcessOneEnumeratedFile(const ListingFileInfo &file) 
  {
    auto emplace_result = known_metrics_files_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(std::move(file.name)),
      std::forward_as_tuple()); // metricsFileData is default-constructed

    auto &data = emplace_result.first->second;

    if (emplace_result.second) {
      // previously existing file (insertion did not happen). We have already
      // read and/or updated dispatchedOffset. Has this file grown?
      if (data.last_known_size < file.size) {
        // yes.
        data.last_known_size = file.size;
        data.dispatch_now = true;
      }
      return;
    }

    const auto &name = emplace_result.first->first;
    // new file. What is the xattr value in there?
    data.stored_dispatched_offset = cluster_.GetDispatchedOffset(name);
    data.bytes_processed = data.stored_dispatched_offset;
    data.last_known_size = file.size;
    // if we dispatched it through the end, don't dispatch again
    data.dispatch_now = (data.bytes_processed < data.last_known_size);
  }

  void Reset()
  {
    const auto now = std::chrono::steady_clock::now();
    const auto cutoff = now - std::chrono::minutes(kMaxMetricsAgeMinutes);

    auto it = unflushed_metrics_.begin();
    while (it != unflushed_metrics_.end()) {
      auto &history = it->second;
      auto next = std::next(it);
      if (!history.timeline.empty()) {
        assert(history.high_water_mark < history.timeline.back().timestamp_);
        history.high_water_mark = history.timeline.back().timestamp_;
        global_high_water_mark_ =
          std::max(history.high_water_mark, global_high_water_mark_);
        history.high_water_time = now;
        history.timeline.clear();
      } else if (history.high_water_time < cutoff) {
        unflushed_metrics_.erase(it);
      }

      it = next;
    }
  }

   void AddEverything(
     Metric *m, 
    const Fid & table, const Fid &index,
    opType op,  int64_t timestamp) const
  {
     m->set_time(timestamp);
     AddTableTag(table, m);
     AddIndexTag(table, index, m);
     AddRpcTag(op, m);
     AddHostTag(m);
   }

  void AddHostTag(Metric *m) const
  {
    auto tag_host = m->add_tags();
    tag_host->set_name("fqdn");
    tag_host->set_value(cluster_.Hostname());
  }

  void AddTableTag(const Fid &key, Metric *m) const
  {
    auto tag_fid = m->add_tags();
    tag_fid->set_name("table_fid");
    tag_fid->set_value(key.c_str());

    auto tag_path = m->add_tags();
    tag_path->set_name("table_path");

    char buffer[PATH_MAX];
    int fid_error = cluster_.GetPathFromFid(key, buffer);
    if (fid_error != 0) {
      ERROR("getPathFromFid(%s) returned %d", key.c_str(), fid_error);
      tag_path->set_value(std::string("//deleted_table_") + key.c_str());
    } else {
      tag_path->set_value(buffer);
    }
  }

  void AddIndexTag(const Fid &table, const Fid &index, Metric *m) const
  {
    if (index.empty()) {
      auto noindex = m->add_tags();
      noindex->set_name("noindex");
      noindex->set_value("//primary");
      return;
    }
    auto tag_index_fid = m->add_tags();
    tag_index_fid->set_name("index_fid");
    tag_index_fid->set_value(index.c_str());

    char buffer[PATH_MAX];
    int fid_error = cluster_.GetIndexNameFromFids(table, index, buffer);
    if (fid_error != 0) {
      ERROR("getIndexNameFromFids(%s, %s) returned %d",
        table.c_str(), index.c_str(), fid_error);
    } else {
      auto tag_path = m->add_tags();
      tag_path->set_name("index");
      tag_path->set_value(buffer);
    }
  }

  void AddRpcTag(opType op, Metric *m) const
  {
    auto tag_rpc = m->add_tags();
    tag_rpc->set_name("rpc_type");
    tag_rpc->set_value(to_c_str(op));
  }

  void Protobufize(
    const Fid &table, const Fid &index, const Timepoint &item,
    Metrics *message) const
  {
    int i = 0;
    for (const auto &rpc : item.numbers_) {
      const auto op = static_cast<opType>(i);

      if (!opType_IsValid(i++)) { // postincrement intended
        continue;
      }

      if (rpc.rpcs == 0) {
        continue;
      }

      // for each RPC we have a bunch of metrics
      for (const auto &metric : rpc.enumerate()) {
        if (metric.value == 0) {
          continue;
        }

        auto m = message->add_metrics();

        m->mutable_value()->set_number(metric.value);
        m->set_name(to_c_str(metric.name));
        AddEverything(m, table, index, op, item.timestamp_);
      }

      // and a histogram. Histogram has several buckets:
      static_assert(kBuckets.size() == decltype(rpc.histo){}.size(), "");
      static_assert(kBuckets.size() ==
        IndexOfLastTableMetricsBucket + 1, "");

      auto mHisto = message->add_metrics();
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
      AddEverything(mHisto, table, index, op, item.timestamp_);
    }

  }
  void Flush() const
  {
    Metrics message;
    for (auto &it : unflushed_metrics_) {
      const auto &key = it.first;
      const auto &table = key.primary_;
      const auto &index = key.si_;
      const auto &backlog = it.second.timeline;
      for (const auto &item : backlog) {
        Protobufize(table, index, item, &message);
      }
    }

    auto cb = message.ByteSize();
    if (cb == 0) {
      // no messages;
      return;
    }
    auto buffer = AllocateMetricsPointer(cb);

    auto bMustSucceed = message.SerializeToArray(buffer->data, cb);
    if (!bMustSucceed) {
      ERR("Failed to serialize the opaque msg to writer, 0n%d bytes.", cb);
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

  struct {
    int64_t eta_;
    int bytes_so_far_;
    bool use_eta_;

    void Reset()
    {
      bytes_so_far_ = 0;
      use_eta_ = false;
    }

    // when returns true, plugin should not process this record at this time
    bool ShouldStop(int bytes, int64_t timestamp)
    {
      if (use_eta_) {
        return timestamp > eta_;
      }

      if (bytes_so_far_ < kIntakeThreshold) {
        eta_ = std::min(eta_, timestamp);
        bytes_so_far_ += bytes;
      } else {
        eta_ = std::max(timestamp, eta_ + 1000 * kBacklogProgressSeconds);
        use_eta_ = true;
      }

      return false;
    }

  } early_finish;


  int Read()
  {
    early_finish.Reset();

    auto success = cluster_.EnumerateMetricsFiles();
    if (!success) {
      return ENOENT;
    }

    for (const auto &file : cluster_.GetMetricsFiles()) {
      ProcessOneEnumeratedFile(file);
    }

    // at this point all files are in knownMetricsFiles_
    for (auto &it : known_metrics_files_) {
      const auto &name = it.first;
      auto &details = it.second;
      if (!details.dispatch_now) {
        // LOG("details.dispatchNow == false, next");
        continue;
      }
      ProcessOneMetricsFile(name, &details);
      cluster_.SetDispatchedOffset(name, details.bytes_processed);
      details.stored_dispatched_offset = details.bytes_processed;
    }

    Flush();
    Reset();

    return 0;
  }

  static TableMetricsPlugin *instance()
  {
    static TableMetricsPlugin s_metrics;
    return &s_metrics;
  }

  std::string log_conf_;

  // config callback happens before anything else. it might tell us whether
  // we have a log file
  static int ConfigCallback(oconfig_item_t *item)
  {
    return instance()->AddConfig(item);
  }

  int AddConfig(oconfig_item_t *config_item)
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
  static void RegisterPlugin()
  {
    ::plugin_register_complex_config(kCollectdPluginName, ConfigCallback);
    ::plugin_register_init(kCollectdPluginName, InitCallback);
    ::plugin_register_read(kCollectdPluginName, ReadCallback);
  }
};

const char TableMetricsPlugin::kCollectdPluginName[] = "mapr_tblmetrics";
const char TableMetricsPlugin::kLogSettingName[] = "Log_Config_File";

extern "C"
void module_register(void)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  TableMetricsPlugin::RegisterPlugin();
} /* void module_register */
