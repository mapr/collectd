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
 *     Stream "/var/mapr/mapr.monitoring/spyglass"
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

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "configfile.h"
#include "utils_cmd_putval.h"
#include "utils_format_graphite.h"
#include "utils_format_json.h"
#include "utils_crc32.h"

#include <stdint.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>
#include <zlib.h>
#include <errno.h>
#include "utils_cache.h"
#include <sys/socket.h>


#ifndef WT_DEFAULT_STREAM
# define WT_DEFAULT_STREAM "/var/mapr/mapr.monitoring/spyglass"
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
#define KAFKA_FORMAT_JSON        0
#define KAFKA_FORMAT_COMMAND     1
    uint8_t                     format;
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
    char                        *clusterId;
    char                        *stream;
    pthread_mutex_t              lock;
};

/*
 * Functions
 */
static int wt_kafka_handle(struct wt_kafka_topic_context *);
static int32_t wt_kafka_partition(const rd_kafka_topic_t *, const void *, size_t,
                               int32_t, void *, void *);

#if defined HAVE_LIBRDKAFKA_LOGGER || defined HAVE_LIBRDKAFKA_LOG_CB
static void wt_kafka_log(const rd_kafka_t *, int, const char *, const char *);

static void wt_kafka_log(const rd_kafka_t *rkt, int level,
                      const char *fac, const char *msg)
{
    plugin_log(level, "%s", msg);
}
#endif

static void wt_kafka_topic_context_free(void *p) /* {{{ */
{
  struct wt_kafka_topic_context *ctx = p;
  INFO("mapr_writemaprstreams plugin: inside context free");
  if (ctx == NULL)
    return;
  if (ctx->clusterId != NULL)
    sfree(ctx->clusterId);
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
  if (ctx->kafka_conf != NULL)
    rd_kafka_conf_destroy(ctx->kafka_conf);
  if (ctx->kafka != NULL)
    rd_kafka_destroy(ctx->kafka);

    sfree(ctx);
} /* }}} void wt_kafka_topic_context_free */

static int32_t wt_kafka_partition(const rd_kafka_topic_t *rkt,
                               const void *keydata, size_t keylen,
                               int32_t partition_cnt, void *p, void *m)
{
    uint32_t key = *((uint32_t *)keydata );
    uint32_t target = key % partition_cnt;
    int32_t   i = partition_cnt;

    while (--i > 0 && !rd_kafka_topic_partition_available(rkt, target)) {
        target = (target + 1) % partition_cnt;
    }
    return target;
}

static int wt_kafka_handle(struct wt_kafka_topic_context *ctx) /* {{{ */
{
    rd_kafka_topic_conf_t       *topic_conf;

    if (ctx->kafka != NULL && ctx->topic != NULL)
        return(0);

    if (ctx->topic == NULL ) {
      if ((topic_conf = rd_kafka_topic_conf_dup(ctx->conf)) == NULL) {
        ERROR("write_maprstreams plugin: cannot duplicate kafka topic config");
        return 1;
      }

      if ((ctx->topic = rd_kafka_topic_new(ctx->kafka, ctx->topic_name,
          topic_conf)) == NULL) {
        ERROR("write_maprstreams plugin: cannot create topic : %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
        return errno;
      }

      rd_kafka_topic_conf_destroy(ctx->conf);
      ctx->conf = NULL;


      INFO ("write_maprstreams plugin: handle created for topic : %s", rd_kafka_topic_name(ctx->topic));
    }

    return(0);

} /* }}} int wt_kafka_handle */


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
        status = ssnprintf (ret + offset, ret_len - offset, \
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
        BUFFER_ADD("%" PRIi64, vl->values[ds_num].derive);
    else if (ds->ds[ds_num].type == DS_TYPE_ABSOLUTE)
        BUFFER_ADD("%" PRIu64, vl->values[ds_num].absolute);
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
                          const struct wt_kafka_topic_context *cb,
                          const char *ds_name)
{
    int status;
    char *temp = NULL;
    char *ptr = ret;
    size_t remaining_len = ret_len;
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
            n = ssnprintf(ptr, remaining_len, " %s=%s", k, v); \
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
                n = ssnprintf(ptr, remaining_len, " %s", temp);
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
                          const struct wt_kafka_topic_context *cb,
                          const char *ds_name)
{
    int status;
    int i;
    char *temp = NULL;
    char *prefix = NULL;
    const char *meta_prefix = "tsdb_prefix";
    char *tsdb_id = NULL;
    const char *meta_id = "tsdb_id";

    _Bool include_in_id[] = {
        /* plugin =          */ 1,
        /* plugin instance = */ (vl->plugin_instance[0] == '\0')?0:1,
        /* type =            */ 1,
        /* type instance =   */ (vl->type_instance[0] == '\0')?0:1,
        /* ds_name =         */ (ds_name == NULL)?0:1
    };

    if (vl->meta) {
        status = meta_data_get_string(vl->meta, meta_prefix, &temp);
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

        for(i=0; i < (sizeof(meta_tag_metric_id)/sizeof(*meta_tag_metric_id)); i++) {
            if(0 == meta_data_exists(vl->meta, meta_tag_metric_id[i])) {
                /* defaults to already initialized format */
            } else {
                include_in_id[i] = 0;
            }
        }
    }
    if(tsdb_id) {
        ssnprintf(ret, ret_len, "%s%s", prefix?prefix:"", tsdb_id);
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

static int wt_send_message (const char* key, const char* value,
                            const char* value_tags,
                            cdtime_t time, struct wt_kafka_topic_context *ctx,
                            const value_list_t *vl)
{
    int status;
    int message_len;
    char *temp = NULL;
    char *tags = "";
    char message[8192];
    char *host_tags = ctx->host_tags ? ctx->host_tags : "";
    const char *meta_tsdb = "tsdb_tags";
    const char* host = vl->host;
    meta_data_t *md = vl->meta;
    uint32_t  partition_key;

    pthread_mutex_lock (&ctx->lock);
    // Allocate enough space for the topic name -- "<streamname>:<fqdn>_<metric name>"
    //char *temp_topic_name = (char *) malloc( strlen(ctx->clusterId) + strlen(ctx->stream) + strlen(host) + strlen(key) + 4 );
    char *temp_topic_name = (char *) malloc( strlen(ctx->stream) + strlen(host) + strlen(key) + 3 );
    strcpy(temp_topic_name,ctx->stream);
    strcat(temp_topic_name,":");
    //strcat(temp_topic_name,ctx->clusterId);
    //strcat(temp_topic_name,"_");
    strcat(temp_topic_name,host);
    strcat(temp_topic_name,"_");
    strcat(temp_topic_name, key);
    strcpy(ctx->topic_name,temp_topic_name);
    INFO("write_maprstreams plugin for key %s stream name %s ",key,ctx->stream);
    INFO("write_maprstreams plugin: topic name %s ",ctx->topic_name);
    // Create conf because it gets set to NULL in wt_kafka_handle call below
    if ((ctx->conf = rd_kafka_topic_conf_new()) == NULL) {
      rd_kafka_conf_destroy(ctx->kafka_conf);
      sfree(ctx);
      ERROR ("write_maprstream plugin: cannot create topic configuration.");
      return -1;
    }
    // Get a handle to kafka topics and kafka conf
    status = wt_kafka_handle(ctx);
    pthread_mutex_unlock (&ctx->lock);
    if( status != 0 )
      return status;
    bzero(message, sizeof(message));

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
            tags = temp;
        }
    }

    message_len = ssnprintf (message,
                             sizeof(message),
                             "put %s %.0f %s fqdn=%s %s %s %s\r\n",
                             key,
                             CDTIME_T_TO_DOUBLE(time),
                             value,
                             host,
                             value_tags,
                             tags,
                             host_tags);

    sfree(temp);

    if (message_len >= sizeof(message)) {
        ERROR("write_maprstreams plugin: message buffer too small: "
              "Need %d bytes.", message_len + 1);
        return -1;
    }

    pthread_mutex_lock(&ctx->lock);

    partition_key = rand();
    // Send the message to topic
    rd_kafka_produce(ctx->topic, RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY, message, sizeof(message),
        &partition_key, sizeof(partition_key), NULL);

    // Free the space allocated for temp topic name
    free(temp_topic_name);
    // Set topic name and topic to null so a new topic conf is created for each messages based on the metric key
    ctx->topic_name = NULL;
    if (ctx->topic != NULL)
      rd_kafka_topic_destroy(ctx->topic);
    ctx->topic = NULL;
    pthread_mutex_unlock(&ctx->lock);

    return 0;
}

static int wt_write_messages(const data_set_t *ds, const value_list_t *vl,
                             struct wt_kafka_topic_context *cb)
{
    char key[10*DATA_MAX_NAME_LEN];
    char values[512];
    char tags[10*DATA_MAX_NAME_LEN];

    int status, i;

    if (0 != strcmp(ds->type, vl->type))
    {
        ERROR("write_maprstreams plugin: DS type does not match "
              "value list type");
        return -1;
    }

    for (i = 0; i < ds->ds_num; i++)
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


        /* Send the message to MapR Streams */
        status = wt_send_message(key, values, tags, vl->time, cb, vl);
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
    INFO("write_maprstreams plugin: user_data %p", user_data->data);
    struct wt_kafka_topic_context  *ctx = user_data->data;
    INFO("write_maprstreams plugin: stream_name %s", ctx->stream);
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
  sfree(tctx);
}

static int wt_config_stream(oconfig_item_t *ci)
{
    user_data_t user_data;
    char callback_name[DATA_MAX_NAME_LEN];
    rd_kafka_conf_t *conf;
    struct wt_kafka_topic_context  *tctx;
    int status;
    char                         errbuf[1024];

    int i;
    if ((conf = rd_kafka_conf_new()) == NULL) {
      ERROR("cannot allocate kafka configuration.");
      return -1;
    }
    if ((tctx = malloc(sizeof (struct wt_kafka_topic_context))) == NULL) {
      ERROR ("write_maprstream plugin: malloc failed.");
      return -1;
    }

    tctx->escape_char = '.';
    tctx->store_rates = 0;
    tctx->format = KAFKA_FORMAT_JSON;
    tctx->stream = NULL;
    tctx->host_tags = NULL;
    tctx->topic_name = NULL;
    tctx->clusterId = NULL;

    if ((tctx->kafka_conf = rd_kafka_conf_dup(conf)) == NULL) {
      sfree(tctx);
      ERROR("write_maprstream plugin: cannot allocate memory for kafka config");
      return -1;
    }

#ifdef HAVE_LIBRDKAFKA_LOG_CB
    rd_kafka_conf_set_log_cb(tctx->kafka_conf, wt_kafka_log);
#endif

    if ((tctx->conf = rd_kafka_topic_conf_new()) == NULL) {
      rd_kafka_conf_destroy(tctx->kafka_conf);
      sfree(tctx);
      ERROR ("write_maprstream plugin: cannot create topic configuration.");
      return -1;
    }

    if (tctx->kafka == NULL) {
      if ((conf = rd_kafka_conf_dup(tctx->kafka_conf)) == NULL) {
        ERROR("write_maprstreams plugin: cannot duplicate kafka config");
        return(1);
      }

      if ((tctx->kafka = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
          errbuf, sizeof(errbuf))) == NULL) {
        ERROR("write_maprstreams plugin: cannot create kafka handle.");
        return 1;
      }

#ifdef HAVE_LIBRDKAFKA_LOGGER
      rd_kafka_conf_set_log_cb(tctx->kafka_conf, wt_kafka_log);
#endif

      rd_kafka_conf_destroy(tctx->kafka_conf);
      tctx->kafka_conf = NULL;

      INFO ("write_maprstreams plugin: created KAFKA handle : %s", rd_kafka_name(tctx->kafka));

    }


    for (i = 0; i < ci->children_num; i++)
    {
        oconfig_item_t *child = ci->children + i;

        if (strcasecmp("Stream", child->key) == 0)
            cf_util_get_string(child, &tctx->stream);
        else if (strcasecmp("HostTags", child->key) == 0)
            cf_util_get_string(child, &tctx->host_tags);
        else
        {
            ERROR("write_maprstreams plugin: Invalid configuration "
                  "option: %s.", child->key);
            clearContext(tctx);
            return -1;
        }
    }

    if (tctx->stream == NULL) {
      ERROR("write_maprstreams plugin: Required parameters Stream is missing in configuration");
      clearContext(tctx);

    }

    rd_kafka_topic_conf_set_partitioner_cb(tctx->conf, wt_kafka_partition);
    rd_kafka_topic_conf_set_opaque(tctx->conf, tctx);

    ssnprintf(callback_name, sizeof(callback_name), "write_maprstreams/%s",
        tctx->stream != NULL ? tctx->stream : WT_DEFAULT_STREAM);

    INFO ("write_maprstreams plugin: stream name %s",tctx->stream);
    INFO ("write_maprstreams plugin: host tags name %s",tctx->host_tags);
    // Get the clusterId from host_tags
    char *clusterIdStr = (char *) malloc (strlen(tctx->host_tags) + 1);
    strcpy(clusterIdStr, tctx->host_tags);
    char *clusterId;
    clusterId = strtok(clusterIdStr, "=");
    if ( clusterId != NULL ) {
      clusterId = strtok(NULL, "=");
    }
    tctx->clusterId = (char *) malloc (strlen(clusterId) + 1);
    strcpy(tctx->clusterId,clusterId);
    INFO ("write_maprstreams plugin: Found clusterId %s", tctx->clusterId);
    memset(&user_data, 0, sizeof(user_data));
    user_data.data = tctx;
    user_data.free_func = wt_kafka_topic_context_free;
    status = plugin_register_write(callback_name, wt_write, &user_data);
    if (status != 0) {
      ERROR ("write_maprstreams plugin: plugin_register_write (\"%s\") "
          "failed with status %i.",
          callback_name, status);
      clearContext(tctx);

    }
    free(clusterIdStr);
    clusterId = NULL;

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

void module_register(void)
{
    plugin_register_complex_config("write_maprstreams", wt_config);
}

/* vim: set sw=4 ts=4 sts=4 tw=78 et : */
