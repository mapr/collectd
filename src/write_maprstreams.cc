/**
 * collectd - src/write_maprstreams.c
 * write_maprstreams Authors:
 *   Naveen Tirupattur <ntirupattur@maprtech.com>
 **/

/* write_maprstreams plugin configuation example
 * --------------------------------------
 *
 * <Plugin write_maprstreams>
 *   <Node>
 *     Path "/var/mapr/mapr.monitoring/streams"
 *     HostTags "clusterId=$clusterId"
 *   </Node>
 * </Plugin>
 *
 * write_maprstreams meta_data
 * --------------------
 *  - tsdb_prefix : Will prefix the OpenTSDB <metric> (also prefix tsdb_id if defined)
 *  - tsdb_id     : Replace the metric with this tag
 *
 *  - tsdb_tag_plugin         : When defined, tsdb_tag_* removes the related
 *  - tsdb_tag_pluginInstance : item from metric id.
 *  - tsdb_tag_type           : If it is not empty, it will be the key of an
 *  - tsdb_tag_typeInstance   : opentsdb tag (the value is the item itself)
 *  - tsdb_tag_dsname         : If it is empty, no tag is defined.
 *
 *  - tsdb_tag                : Should contain "tagk=tagv". Il will add a tag.
 *
 * write_maprstreams plugin filter rules example
 * --------------------------------------
 *
 * <Chain "PreCache">
 *   <Rule "opentsdb_cpu">
 *     <Match "regex">
 *       Plugin "^cpu$"
 *     </Match>
 *     <Target "set">
 *       MetaDataSet "tsdb_tag_pluginInstance" "cpu"
 *       MetaDataSet "tsdb_tag_type" ""
 *       MetaDataSet "tsdb_prefix" "sys."
 *     </Target>
 *   </Rule>
 *   <Rule "opentsdb_df">
 *     <Match "regex">
 *       Plugin "^df$"
 *     </Match>
 *     <Target "set">
 *       MetaDataSet "tsdb_tag_pluginInstance" "mount"
 *       MetaDataSet "tsdb_tag_type" ""
 *       MetaDataSet "tsdb_prefix" "sys."
 *     </Target>
 *   </Rule>
 *   <Rule "opentsdb_disk">
 *     <Match "regex">
 *       Plugin "^disk$"
 *     </Match>
 *     <Target "set">
 *       MetaDataSet "tsdb_tag_pluginInstance" "disk"
 *       MetaDataSet "tsdb_prefix" "sys."
 *     </Target>
 *   </Rule>
 *   <Rule "opentsdb_interface">
 *     <Match "regex">
 *       Plugin "^interface$"
 *     </Match>
 *     <Target "set">
 *       MetaDataSet "tsdb_tag_pluginInstance" "iface"
 *       MetaDataSet "tsdb_prefix" "sys."
 *     </Target>
 *   </Rule>
 *   <Rule "opentsdb_load">
 *     <Match "regex">
 *       Plugin "^loac$"
 *     </Match>
 *     <Target "set">
 *       MetaDataSet "tsdb_tag_type" ""
 *       MetaDataSet "tsdb_prefix" "sys."
 *     </Target>
 *   </Rule>
 *   <Rule "opentsdb_swap">
 *     <Match "regex">
 *       Plugin "^swap$"
 *     </Match>
 *     <Target "set">
 *       MetaDataSet "tsdb_prefix" "sys."
 *     </Target>
 *   </Rule>
 * </Chain>
 *
 * IMPORTANT WARNING
 * -----------------
 * OpenTSDB allows no more than 8 tags.
 * Collectd admins should be aware of this when defining filter rules and host
 * tags.
 *
 */

#include <cinttypes>

#include <stdbool.h>

extern "C" {
#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "configfile.h"
#include "utils_format_json.h"
#include "utils_cache.h"
}

#include <new>
#include <librdkafka/rdkafka.h>
#include <pthread.h>

#include <errno.h>
#include <assert.h>

#include "mapr_metrics.h"
#include "mapr_metrics.pb.h"


#ifndef WT_DEFAULT_PATH
# define WT_DEFAULT_PATH "/var/mapr/mapr.monitoring/streams"
#endif

#ifndef WT_DEFAULT_ESCAPE
# define WT_DEFAULT_ESCAPE '.'
#endif

/* Ethernet - (IPv6 + TCP) = 1500 - (40 + 32) = 1428 */
#ifndef WT_SEND_BUF_SIZE
# define WT_SEND_BUF_SIZE 1428
#endif

/* Meta data definitions about tsdb tags */
#define TSDB_TAG_PLUGIN 0
#define TSDB_TAG_PLUGININSTANCE 1
#define TSDB_TAG_TYPE 2
#define TSDB_TAG_TYPEINSTANCE 3
#define TSDB_TAG_DSNAME 4
static const char *meta_tag_metric_id[] = {
    "tsdb_tag_plugin",
    "tsdb_tag_pluginInstance",
    "tsdb_tag_type",
    "tsdb_tag_typeInstance",
    "tsdb_tag_dsname"
};

/*
 * Private variables
 */
struct wt_kafka_topic_context {
    _Bool                       store_rates;
    rd_kafka_topic_conf_t       *conf;
    rd_kafka_topic_t            *topic;
    rd_kafka_conf_t             *kafka_conf;
    rd_kafka_t                  *kafka;
    int                          has_key;
    uint32_t                     key;
    char                         escape_char;
    char                        *topic_name;
    char                        *host_tags;
    char                        *preprocessed_host_tags;
    char                        *stream;
    char						*path;
    int 						 streamsCount;
    pthread_mutex_t              lock;
};

/*
 * Functions
 */
static int wt_kafka_handle(struct wt_kafka_topic_context *);
//static int32_t wt_kafka_partition(const rd_kafka_topic_t *, const void *, size_t,
//                               int32_t, void *, void *);

#if defined HAVE_LIBRDKAFKA_LOGGER || defined HAVE_LIBRDKAFKA_LOG_CB
static void wt_kafka_log(const rd_kafka_t *, int, const char *, const char *);

static void wt_kafka_log(const rd_kafka_t *rkt, int level,
                      const char *fac, const char *msg)
{
    plugin_log(level, "%s", msg);
}
#endif

/*
 * djb2 hash algorithm
 */
static int hash(const char *str, int range)
{
	int hash = 5381;
	int c;
	while ((c = *str++) != 0)
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
	return abs(hash%range);
}

/* msgDeliveryCB: Is the delivery callback.
 * The delivery report callback will be called once for each message
 * accepted by rd_kafka_produce() with err set to indicate
 * the result of the produce request. An application must call rd_kafka_poll()
 * at regular intervals to serve queued delivery report callbacks.
 */
static void msgDeliveryCB (rd_kafka_t *,
                           const rd_kafka_message_t *rkmessage, void *) {
    if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        ERROR("write_maprstreams plugin: FAILURE: Message not delivered to partition.\n");
        ERROR("write_maprstreams plugin: ERROR: %s", rd_kafka_err2str(rkmessage->err));
    } else {
        INFO("write_maprstreams plugin: Produced: %.*s\n",(int)rkmessage->len, (const char*)rkmessage->payload);
    }
    //free((rd_kafka_message_t*)rkmessage);
}

static void wt_kafka_topic_context_free(void *p) /* {{{ */
{
  auto ctx = static_cast<wt_kafka_topic_context *>(p);
  INFO("mapr_writemaprstreams plugin: inside context free");
  if (ctx == NULL)
    return;
  //pthread_mutex_lock (&ctx->lock); // Bug - 29675 - Removing the lock because this function is called by only one thread
  if (ctx->topic_name != NULL)
    sfree(ctx->topic_name);
  if (ctx->stream != NULL)
    sfree(ctx->stream);
  if (ctx->host_tags != NULL)
    sfree(ctx->host_tags);
  if (ctx->topic != NULL)
    rd_kafka_topic_destroy(ctx->topic);
  if (ctx->conf != NULL)
    rd_kafka_topic_conf_destroy(ctx->conf);
// Bug - 25911
//  if (ctx->kafka_conf != NULL)
//    rd_kafka_conf_destroy(ctx->kafka_conf);
  /* Wait for messages to be delivered */
  while (rd_kafka_outq_len(ctx->kafka) > 0)
    rd_kafka_poll(ctx->kafka, 100);
  if (ctx->kafka != NULL)
    rd_kafka_destroy(ctx->kafka);
  //pthread_mutex_destroy(&ctx->lock); // Bug - 29675 - Removing the lock because this function is called by only one thread
    sfree(ctx);
} /* }}} void wt_kafka_topic_context_free */

static int wt_kafka_handle(struct wt_kafka_topic_context *ctx) /* {{{ */
{
    rd_kafka_topic_conf_t       *topic_conf;
    rd_kafka_conf_t *conf;
    char                         errbuf[1024];

    if (ctx->kafka != NULL && ctx->topic != NULL)
        return(0);

    if (ctx->kafka == NULL) {
      if ((conf = rd_kafka_conf_dup(ctx->kafka_conf)) == NULL) {
        ERROR("write_maprstreams plugin: cannot duplicate kafka config");
        return(1);
      }

      rd_kafka_conf_set_dr_msg_cb(conf, msgDeliveryCB);

      if ((ctx->kafka = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
           errbuf, sizeof(errbuf))) == NULL) {
        ERROR("write_maprstreams plugin: cannot create kafka handle.");
        return 1;
      }

#ifdef HAVE_LIBRDKAFKA_LOGGER
   rd_kafka_conf_set_log_cb(ctx->kafka_conf, wt_kafka_log);
#endif

    // Bug - 25911
    //rd_kafka_conf_destroy(tctx->kafka_conf);
    //tctx->kafka_conf = NULL;

      INFO ("write_maprstreams plugin: created KAFKA handle : %s", rd_kafka_name(ctx->kafka));

    }

    if (ctx->topic == NULL ) {
      if ((topic_conf = rd_kafka_topic_conf_dup(ctx->conf)) == NULL) {
        ERROR("write_maprstreams plugin: cannot duplicate kafka topic config");
        return 1;
      }

      if ((ctx->topic = rd_kafka_topic_new(ctx->kafka, ctx->topic_name,
          topic_conf)) == NULL) {
        ERROR("write_maprstreams plugin: cannot create topic : %s\n",
            rd_kafka_err2str(rd_kafka_last_error()));
        return errno;
      }

      rd_kafka_topic_conf_destroy(ctx->conf);
      ctx->conf = NULL;
      // Uncomment this line once bug 30736 is fixed
      INFO("write_maprstreams plugin: handle created for topic : %s", rd_kafka_topic_name(ctx->topic));
    }

    return(0);

} /* }}} int wt_kafka_handle */

inline const char * format_PRIi64()
{
    return "%" PRIi64;
}

inline const char * format_PRIu64()
{
    return "%" PRIu64;
}

static int wt_format_values(char *ret, size_t ret_len,
                            int ds_num, const data_set_t *ds,
                            const value_list_t *vl,
                            _Bool store_rates)
{
    size_t offset = 0;
    int status;
    gauge_t *rates = NULL;

    assert(0 == strcmp (ds->type, vl->type));

    memset(ret, 0, ret_len);

#define BUFFER_ADD(...) do { \
        status = snprintf (ret + offset, ret_len - offset, \
                            __VA_ARGS__); \
        if (status < 1) \
        { \
            sfree(rates); \
            return -1; \
        } \
        else if (((size_t) status) >= (ret_len - offset)) \
        { \
            sfree(rates); \
            return -1; \
        } \
        else \
            offset += ((size_t) status); \
} while (0)

    if (ds->ds[ds_num].type == DS_TYPE_GAUGE)
        BUFFER_ADD(GAUGE_FORMAT, vl->values[ds_num].gauge);
    else if (store_rates)
    {
        if (rates == NULL)
            rates = uc_get_rate (ds, vl);
        if (rates == NULL)
        {
            WARNING("format_values: "
                    "uc_get_rate failed.");
            return -1;
        }
        BUFFER_ADD(GAUGE_FORMAT, rates[ds_num]);
    }
    else if (ds->ds[ds_num].type == DS_TYPE_COUNTER)
        BUFFER_ADD("%llu", vl->values[ds_num].counter);
    else if (ds->ds[ds_num].type == DS_TYPE_DERIVE)
        BUFFER_ADD(format_PRIi64(), vl->values[ds_num].derive);
    else if (ds->ds[ds_num].type == DS_TYPE_ABSOLUTE)
        BUFFER_ADD(format_PRIu64(), vl->values[ds_num].absolute);
    else
    {
        ERROR("format_values plugin: Unknown data source type: %i",
              ds->ds[ds_num].type);
        sfree(rates);
        return -1;
    }

#undef BUFFER_ADD

    sfree(rates);
    return 0;
}

static int wt_format_tags(char *ret, int ret_len,
                          const value_list_t *vl,
                          const struct wt_kafka_topic_context *,
                          const char *ds_name)
{
    int status;
    char *temp = NULL;
    char *ptr = ret;
    ptrdiff_t remaining_len = ret_len;
    const char *meta_tag = "tsdb_tag";

#define TSDB_META_DATA_GET_STRING(tag) do { \
        temp = NULL; \
        status = meta_data_get_string(vl->meta, tag, &temp); \
        if (status == -ENOENT) { \
            temp = NULL; \
            /* defaults to empty string */ \
        } else if (status < 0) { \
            sfree(temp); \
            return status; \
        } \
    } while(0)

#define TSDB_STRING_APPEND_SPRINTF(key, value) do { \
        int n; \
        const char *k = (key); \
        const char *v = (value); \
        if(k[0] != '\0' && v[0] != '\0') { \
            n = snprintf(ptr, remaining_len, " %s=%s", k, v); \
            if(n >= remaining_len) { \
                ptr[0] = '\0'; \
            } else { \
                char *ptr2 = ptr+1; \
                while(NULL != (ptr2 = strchr(ptr2, ' '))) ptr2[0] = '_';  \
                ptr += n; \
                remaining_len -= n; \
            } \
        } \
    } while(0)

    if (vl->meta) {
        TSDB_META_DATA_GET_STRING(meta_tag_metric_id[TSDB_TAG_PLUGIN]);
        if(temp) {
            TSDB_STRING_APPEND_SPRINTF(temp, vl->plugin);
            sfree(temp);
        }

        TSDB_META_DATA_GET_STRING(meta_tag_metric_id[TSDB_TAG_PLUGININSTANCE]);
        if(temp) {
            TSDB_STRING_APPEND_SPRINTF(temp, vl->plugin_instance);
            sfree(temp);
        }

        TSDB_META_DATA_GET_STRING(meta_tag_metric_id[TSDB_TAG_TYPE]);
        if(temp) {
            TSDB_STRING_APPEND_SPRINTF(temp, vl->type);
            sfree(temp);
        }

        TSDB_META_DATA_GET_STRING(meta_tag_metric_id[TSDB_TAG_TYPEINSTANCE]);
        if(temp) {
            TSDB_STRING_APPEND_SPRINTF(temp, vl->type_instance);
            sfree(temp);
        }

        if(ds_name) {
            TSDB_META_DATA_GET_STRING(meta_tag_metric_id[TSDB_TAG_DSNAME]);
            if(temp) {
                TSDB_STRING_APPEND_SPRINTF(temp, ds_name);
                sfree(temp);
            }
        }

        TSDB_META_DATA_GET_STRING(meta_tag);
        if(temp) {
            int n;
            if(NULL == strchr(temp, '=')) {
                ERROR("write_maprstreams plugin: meta_data tag '%s' does not contain a '=' char (host=%s, plugin=%s, type=%s)",
                        temp, vl->host, vl->plugin, vl->type);
                sfree(temp);
            }
            if(temp[0] != '\0') {
                n = snprintf(ptr, remaining_len, " %s", temp);
                if(n >= remaining_len) {
                    ptr[0] = '\0';
                } else {
                    char *ptr2 = ptr+1;
                    while(NULL != (ptr2 = strchr(ptr2, ' '))) ptr2[0] = '_';
                    ptr += n;
                    remaining_len -= n;
                }
            }
            sfree(temp);
        }

    } else {
        ret[0] = '\0';
    }

#undef TSDB_META_DATA_GET_STRING
#undef TSDB_STRING_APPEND_SPRINTF

    return 0;
}

static int wt_format_name(char *ret, int ret_len,
                          const value_list_t *vl,
                          const struct wt_kafka_topic_context *,
                          const char *ds_name)
{
    char *temp = NULL;
    char *prefix = NULL;
    const char *meta_prefix = "tsdb_prefix";
    char *tsdb_id = NULL;
    const char *meta_id = "tsdb_id";

    bool include_in_id[] = {
        /* plugin =          */ true,
        /* plugin instance = */ vl->plugin_instance[0] != '\0',
        /* type =            */ true,
        /* type instance =   */ vl->type_instance[0] != '\0',
        /* ds_name =         */ ds_name != NULL
    };

    if (vl->meta) {
        int status = meta_data_get_string(vl->meta, meta_prefix, &temp);
        if (status == -ENOENT) {
            /* defaults to empty string */
        } else if (status < 0) {
            sfree(temp);
            return status;
        } else {
            prefix = temp;
        }

        status = meta_data_get_string(vl->meta, meta_id, &temp);
        if (status == -ENOENT) {
            /* defaults to empty string */
        } else if (status < 0) {
            sfree(temp);
            return status;
        } else {
            tsdb_id = temp;
        }

        for (size_t i = 0; i < STATIC_ARRAY_SIZE(meta_tag_metric_id); ++i) {
            if (0 == meta_data_exists(vl->meta, meta_tag_metric_id[i])) {
                /* defaults to already initialized format */
            } else {
                include_in_id[i] = false;
            }
        }
    }
    if(tsdb_id) {
        snprintf(ret, ret_len, "%s%s", prefix?prefix:"", tsdb_id);
    } else {
#define TSDB_STRING_APPEND_STRING(string) do { \
    const char *str = (string); \
    size_t len = strlen(str); \
    if(len > (remaining_len - 1)) { \
        ptr[0] = '\0'; \
        return(-ENOSPC); \
    } \
    if(len > 0) {  \
        memcpy(ptr, str, len); \
        ptr += len; \
        remaining_len -= len; \
    } \
} while(0)

#define TSDB_STRING_APPEND_DOT do { \
    if(remaining_len > 2) {  \
        ptr[0] = '.'; \
        ptr ++; \
        remaining_len --; \
    } else {\
        ptr[0] = '\0'; \
        return(-ENOSPC); \
    } \
} while(0)

        char *ptr = ret;
        size_t remaining_len = ret_len;
        if(prefix) {
            TSDB_STRING_APPEND_STRING(prefix);
        }
        if(include_in_id[TSDB_TAG_PLUGIN]) {
            TSDB_STRING_APPEND_STRING(vl->plugin);
        }

        if(include_in_id[TSDB_TAG_PLUGININSTANCE]) {
            TSDB_STRING_APPEND_DOT;
            TSDB_STRING_APPEND_STRING(vl->plugin_instance);
        }
        if(include_in_id[TSDB_TAG_TYPE]) {
            TSDB_STRING_APPEND_DOT;
            TSDB_STRING_APPEND_STRING(vl->type);
        }
        if(include_in_id[TSDB_TAG_TYPEINSTANCE]) {
            TSDB_STRING_APPEND_DOT;
            TSDB_STRING_APPEND_STRING(vl->type_instance);
        }
        if(include_in_id[TSDB_TAG_DSNAME]) {
            TSDB_STRING_APPEND_DOT;
            TSDB_STRING_APPEND_STRING(ds_name);
        }
        ptr[0] = '\0';
#undef TSDB_STRING_APPEND_STRING
#undef TSDB_STRING_APPEND_DOT
    }

    sfree(tsdb_id);
    sfree(prefix);
    return 0;
}

static int wt_send_message (char *message, size_t mlen, cdtime_t time, const char* host, struct wt_kafka_topic_context *ctx)
{
    int status;
    int hashCode;
    int nDigits;

    pthread_mutex_lock (&ctx->lock);
    // Generate a hash between 0 and M for the metric
    hashCode = hash(host,ctx->streamsCount);
    if (hashCode == 0) {
       nDigits = 1;
    } else {
      nDigits = floor(log10(abs(hashCode))) + 1;
      // hashcode is int
      // therefore 10 decimal digits max
      // therefore its log10 is at most 10
      // so nDigits is at most 11
    }

    char *stream_name = (char *) malloc( strlen(ctx->path) + nDigits + 2 );
    strcpy(stream_name,ctx->path);
    strcat(stream_name,"/");
    char append[11];
    sprintf(append,"%d",hashCode);
    strcat(stream_name,append);
    ctx->stream = stream_name;
    INFO("write_maprstreams plugin: Stream Name is %s for message %s",ctx->stream, message);

    // Allocate enough space for the topic name -- "<streamname>:<fqdn>"
    char *temp_topic_name = (char *) malloc( strlen(ctx->stream) + strlen(host) + 2 );
    strcpy(temp_topic_name,ctx->stream);
    strcat(temp_topic_name,":");
    strcat(temp_topic_name,host);

    ctx->topic_name = temp_topic_name;
    //INFO("write_maprstreams plugin for key %s stream name %s ",key,ctx->stream);
    //INFO("write_maprstreams plugin: topic name %s ",ctx->topic_name);
    // Create conf because it gets set to NULL in wt_kafka_handle call below
    if ((ctx->conf = rd_kafka_topic_conf_new()) == NULL) {
      rd_kafka_conf_destroy(ctx->kafka_conf);
      sfree(ctx);
      ERROR ("write_maprstream plugin: cannot create topic configuration.");
      return -1;
    }
    // Get a handle to kafka topics and kafka conf

    status = wt_kafka_handle(ctx);
    if( status != 0 )
      return status;

    // Send the message to topic
    rd_kafka_producev (ctx->kafka,
                          RD_KAFKA_V_RKT(ctx->topic),
                          RD_KAFKA_V_VALUE(message, mlen),
                          RD_KAFKA_V_MSGFLAGS (RD_KAFKA_MSG_F_COPY),
                          RD_KAFKA_V_TIMESTAMP(CDTIME_T_TO_MS(time)),
                          RD_KAFKA_V_END);

    rd_kafka_poll(ctx->kafka,10);

    INFO("write_maprstreams plugin: PRINT message %s of size %zu sent to topic %s",message, mlen, rd_kafka_topic_name(ctx->topic));
    // Free the space allocated for temp topic name and stream name
    free(temp_topic_name);
    free(stream_name);
    // Set topic name and topic to null so a new topic conf is created for each messages based on the metric key
    ctx->topic_name = NULL;
    ctx->stream = NULL;
    if (ctx->topic != NULL)
      rd_kafka_topic_destroy(ctx->topic);
    ctx->topic = NULL;
    pthread_mutex_unlock(&ctx->lock);

    return 0;
}

static int wt_make_send_message (const char* key, const char* value,
                            const char* value_tags,
                            cdtime_t time, struct wt_kafka_topic_context *ctx,
                            const value_list_t *vl)
{
    int status;
    char *temp = NULL;
    const char *tags = "";
    char message[8192];
    const char *host_tags = ctx->host_tags ? ctx->host_tags : "";
    const char *meta_tsdb = "tsdb_tags";
    const char* host = vl->host;
    meta_data_t *md = vl->meta;
    size_t mfree = sizeof(message);
    size_t mfill = 0;
    size_t mlen = 0;

    /* skip if value is NaN */
    if (value[0] == 'n')
        return 0;

    if (md) {
        status = meta_data_get_string(md, meta_tsdb, &temp);
        if (status == -ENOENT) {
            /* defaults to empty string */
        } else if (status < 0) {
            ERROR("write_maprstreams plugin: tags metadata get failure");
            sfree(temp);
            pthread_mutex_unlock(&ctx->lock);
            return status;
        } else {
            INFO("write_maprstreams plugin: metadata found %s ", tags);
            tags = temp;
        }
    }

    format_json_initialize(message, &mfill, &mfree);
    format_json_mapr_data(message, &mfill, &mfree, key, value, host, value_tags, tags, host_tags);
    format_json_finalize(message, &mfill, &mfree);
    mlen = strlen(message);
    INFO("write_maprstreams plugin: json message %s of size %zu",message,mlen);
    sfree(temp);
    status = wt_send_message(message, mlen, time, host, ctx);
    if (status != 0) return status;
    return 0;
}

char *dump_tags_to_json(
    char *dump_at,
    const google::protobuf::RepeatedPtrField<Tag> & tags)
{
    char *p = dump_at;
    for (const auto &tag : tags) {
        sprintf(p, "\"%s\": \"%s\",", tag.name().c_str(), tag.value().c_str());
        p = &p[strlen(p)];
    }
    return p;
}


char *mapr_dump_to_json(
    char *dump_at,
    const Metric &m,
    const google::protobuf::RepeatedPtrField<Tag> &common_tags,
    char *preprocessed_host_tags)
{
    char *p = dump_at;
    sprintf(p, "[{\"metric\": \"%s\",", m.name().c_str());
    p = &p[strlen(p)];

    if (m.value().has_number()) {
        sprintf(p, "\"value\": %" PRIu64, m.value().number());
    } else {
        assert(m.value().buckets_size() > 0);
        strcpy(p, "\"buckets\": {");
        p = &p[strlen(p)];
        for (const auto &bucket : m.value().buckets()) {
            sprintf(
                p, "\"%" PRId64 ",%" PRId64 "\": %" PRId64 ",",
                bucket.start(), bucket.end(), bucket.number());
            p = &p[strlen(p)];
        }
        --p; // comma again
        strcpy(p, "}");
    }
    p = &p[strlen(p)];

    auto have_tags = (m.tags_size() != 0) ||
                     (common_tags.size() != 0) ||
                     (preprocessed_host_tags[0] != '\0');

    if (have_tags) {
        strcpy(p, ",\"tags\": {");
        p = &p[strlen(p)];
        p = dump_tags_to_json(p, common_tags);
        p = dump_tags_to_json(p, m.tags());
        strcpy(p, preprocessed_host_tags);
        p = &p[strlen(p)];
        --p; // to eat the comma
        strcpy(p, "}}]");
    } else {
        strcpy(p, "}]");
    }
    return &p[strlen(p)];
}


void wt_process_and_write_unpacked_metrics(
    const Metrics &metrics,
    const char *host_from_vl, 
    struct wt_kafka_topic_context *ctx)
{
    char staging_buffer[512000];
    char *staging_pointer = &staging_buffer[0];

    // validate common tags:
    INFO("validating 0n%d common tags", metrics.commontags_size());
    for (const auto &common_tag : metrics.commontags()) {
        if (!common_tag.has_name()) {
            // bug in producer.
            ERROR("common tag has no name");
            return;
        }

        if (!common_tag.has_value()) {
            ERROR("common tag has no value");
            return;
        }
    }
    DEBUG("validated 0n%d common tags", metrics.commontags_size());

    for (const auto &one_metric : metrics.metrics()) {
        // each metric will produce one record.
        assert(one_metric.has_time());
        if (!one_metric.has_time()) {
            // malformed protobuf, bug in producer
            return;
        }
        assert(one_metric.has_name());
        if (!one_metric.has_name()) {
            // malformed protobuf, bug in producer
            return;
        }
        DEBUG("validated name %s and time %" PRIu64 ".",
          one_metric.name().c_str(), one_metric.time);

        for (const auto &metric_tag : one_metric.tags()) {
            assert(metric_tag.has_name() && metric_tag.has_value());

            if (!metric_tag.has_name()) {
                ERROR("no tag name");
                return;
            }

            if (!metric_tag.has_value()) {
                ERROR("no tag value");
                return;
            }
        }
        DEBUG("validated %zu tags", one_metric.tags_size());

        const auto &value = one_metric.value();

        // metric:
        //
        // one_metric->name is the name, won't be NULL
        // the value is a number or a histo, described below.
        //
        // tags come in two sets: common tags and normal tags.
        // there are metrics->commontags_size (maybe zero) of
        // metrics->commonTags, they each gave a name and a value, neither
        // NULL;
        // there are one_metric->tags_size() (maybe zero) of non-common tags.
        // they also have a name and value each, and neither is ever NULL.
        //
        // the value of the metric is determined below:

        if ((value.buckets_size() == 0) && !value.has_number()) {
            INFO("Neither value nor buckets for a metric");
            continue;
        }

        if (value.buckets_size()) {
            DEBUG("%zd buckets in histo", value.buckets_size());
            for (const auto &bucket : value.buckets()) {
                auto valid = bucket.has_number() && 
                    bucket.has_start() && bucket.has_end();
                assert(valid);
                if (!valid) {
                    // malformed protobuf, bug on producer side
                    INFO("BUGBUG 12");
                    return;
                }
            }
            // the metric is a histogram. The buckets are right above. There
            // are value->n_buckets. Each has a start, and end, and a number.
            // Number is never zero.
        }

        // INFO("Dumping one metric to buffer at 0x%p", staging_pointer);
        char *temp = mapr_dump_to_json(staging_pointer, one_metric, metrics.commontags(), ctx->preprocessed_host_tags);
        DEBUG("jsoned metric: %s", staging_pointer);
        (void)wt_send_message(
            staging_pointer, temp - staging_pointer, 
             MS_TO_CDTIME_T(one_metric.time()), host_from_vl, ctx);

    }
}


void wt_process_and_write_opaque_buffer(
    const MetricsBuffer *ptr,
    const char *host_from_value_list, 
    struct wt_kafka_topic_context *ctx)
{
    assert(ptr != NULL);
    if (!ptr) {
        return;
    }

    INFO("processing opaque buffer, %d bytes", ptr->bytes);
    Metrics metrics;
    bool success = metrics.ParseFromArray(ptr->data, ptr->bytes);
    if (!success) {
        ERROR("Failed to unpack the opaque protobuf");
        return;
    }

    wt_process_and_write_unpacked_metrics(metrics, host_from_value_list, ctx);
}


static int wt_write_messages(const data_set_t *ds, const value_list_t *vl,
                             struct wt_kafka_topic_context *cb)
{
    char key[10*DATA_MAX_NAME_LEN];
    char values[512];
    char tags[10*DATA_MAX_NAME_LEN];

    int status;

    MetricsBuffer *ptr = DecodeMetricsPointer(vl);
    if (ptr != NULL) {
        wt_process_and_write_opaque_buffer(ptr, vl->host, cb);
        ReleaseMetricsPointer(ptr);
        return 0;
    }

    if (0 != strcmp(ds->type, vl->type))
    {
        ERROR("write_maprstreams plugin: DS type does not match "
              "value list type");
        return -1;
    }

    for (size_t i = 0; i < ds->ds_num; ++i)
    {
        const char *ds_name = NULL;

        if (ds->ds_num > 1)
            ds_name = ds->ds[i].name;

        /* Copy the identifier to 'key' and escape it. */
        status = wt_format_name(key, sizeof(key), vl, cb, ds_name);
        if (status != 0)
        {
            ERROR("write_maprstreams plugin: error with format_name");
            return status;
        }

        escape_string(key, sizeof(key));
        /* Convert the values to an ASCII representation and put that into
         * 'values'. */
        status = wt_format_values(values, sizeof(values), i, ds, vl,
                                  cb->store_rates);
        if (status != 0)
        {
            ERROR("write_maprstreams plugin: error with "
                  "wt_format_values");
            return status;
        }

        /* Copy tags from p-pi/t-ti ds notation into tags */
        tags[0] = '\0';
        status = wt_format_tags(tags, sizeof(tags), vl, cb, ds_name);
        if (status != 0)
        {
            ERROR("write_maprstreams plugin: error with format_tags");
            return status;
        }


        /* Create the JSON message and send it to MapR streams*/
        status = wt_make_send_message(key, values, tags, vl->time, cb, vl);
        if (status != 0)
        {
            ERROR("write_maprstreams plugin: error with "
                  "wt_send_message");
            return status;
        }
    }

    return 0;
}

static int wt_write(const data_set_t *ds, const value_list_t *vl,
                    user_data_t *user_data)
{
    int       status;
    //INFO("write_maprstreams plugin: user_data %p", user_data->data);
    auto *ctx = static_cast<wt_kafka_topic_context *>(user_data->data);
    //INFO("write_maprstreams plugin: stream_name %s", ctx->stream);
    if ((ds == NULL) || (vl == NULL) || (ctx == NULL))
      return EINVAL;

    status = wt_write_messages(ds, vl, ctx);

    return status;
}

static void clearContext(struct wt_kafka_topic_context  *tctx) {
  if (tctx->conf != NULL)
    rd_kafka_topic_conf_destroy(tctx->conf);
  if (tctx->kafka_conf != NULL)
    rd_kafka_conf_destroy(tctx->kafka_conf);
  delete tctx;
}

static void convert_host_tags_to_json(const char *source, char **json)
{
    assert(source);
    assert(json);
    char *temp = strdup(source);
    char *ret = (char *) malloc(strlen(source) * 2);
    ret[0] = '\0';
    char *pos = ret;
    char *name = NULL;
    char *tok = strtok(temp, "= ");

    for (int counter = 1; tok != NULL; ++counter) {
        if (counter % 2) { // name
            name = tok;
        } else { // value
            sprintf(pos, "\"%s\" : \"%s\",", name, tok);
            pos = &pos[strlen(pos)];
        }

        tok = strtok(NULL, "= ");
    }

    free(temp);

    INFO ("preprocessed host tags: %s", ret);
    *json = ret;
}


static int wt_config_stream(oconfig_item_t *ci)
{
    user_data_t user_data;
    char callback_name[DATA_MAX_NAME_LEN];
    rd_kafka_conf_t *conf;
    int status;

    int i;
    if ((conf = rd_kafka_conf_new()) == NULL) {
      ERROR("cannot allocate kafka configuration.");
      return -1;
    }

    auto tctx = new(std::nothrow) wt_kafka_topic_context;
    if (tctx == nullptr) {
      ERROR ("write_maprstream plugin: malloc failed.");
      return -1;
    }

    memset(tctx, 0, sizeof(wt_kafka_topic_context));
    tctx->escape_char = '.';

    if ((tctx->kafka_conf = rd_kafka_conf_dup(conf)) == NULL) {
      delete tctx;
      ERROR("write_maprstream plugin: cannot allocate memory for kafka config");
      return -1;
    }

#ifdef HAVE_LIBRDKAFKA_LOG_CB
    rd_kafka_conf_set_log_cb(tctx->kafka_conf, wt_kafka_log);
#endif

    if ((tctx->conf = rd_kafka_topic_conf_new()) == NULL) {
      rd_kafka_conf_destroy(tctx->kafka_conf);
      delete tctx;
      ERROR ("write_maprstream plugin: cannot create topic configuration.");
      return -1;
    }

    for (i = 0; i < ci->children_num; i++)
    {
      oconfig_item_t *child = ci->children + i;

      if (strcasecmp("Path", child->key) == 0)
        cf_util_get_string(child, &tctx->path);
      else if (strcasecmp("HostTags", child->key) == 0) {
        cf_util_get_string(child, &tctx->host_tags);
        convert_host_tags_to_json(tctx->host_tags, &tctx->preprocessed_host_tags);
      }  else if (strcasecmp("StreamsCount", child->key) == 0)
        cf_util_get_int(child, &tctx->streamsCount);
      else
      {
        ERROR("write_maprstreams plugin: Invalid configuration "
            "option: %s.", child->key);
        clearContext(tctx);
        return -1;
      }
    }

    if (tctx->path == NULL) {
      ERROR("write_maprstreams plugin: Required parameters streams base path is missing in configuration");
      clearContext(tctx);
      return -1;
    }

    snprintf(callback_name, sizeof(callback_name), "write_maprstreams/%s",
        tctx->path != NULL ? tctx->path : WT_DEFAULT_PATH);

    INFO ("write_maprstreams plugin: streams base path %s",tctx->path);
    INFO ("write_maprstreams plugin: host tags name %s",tctx->host_tags);
    memset(&user_data, 0, sizeof(user_data));
    user_data.data = tctx;
    user_data.free_func = wt_kafka_topic_context_free;
    status = plugin_register_write(callback_name, wt_write, &user_data);
    if (status != 0) {
      ERROR ("write_maprstreams plugin: plugin_register_write (\"%s\") "
          "failed with status %i.",
          callback_name, status);
      clearContext(tctx);
      return -1;
    }

    pthread_mutex_init (&tctx->lock, /* attr = */ NULL);
    return 0;
}

static int wt_config(oconfig_item_t *ci)
{
    int i;

    for (i = 0; i < ci->children_num; i++)
    {
        oconfig_item_t *child = ci->children + i;

        if (strcasecmp("Node", child->key) == 0)
            wt_config_stream(child);
        else
        {
            ERROR("write_maprstreams plugin: Invalid configuration "
                  "option: %s.", child->key);
        }
    }

    return 0;
}

extern "C" void module_register(void)
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    plugin_register_complex_config("write_maprstreams", wt_config);
}

/* vim: set sw=4 ts=4 sts=4 tw=78 et : */

