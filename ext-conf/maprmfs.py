#!/usr/bin/env python
import datetime
import os
import subprocess
from random import random

import collectd

INTERVAL = 10

collectd.info("Starting MapR MFS Plugin for Python")


# noinspection PyBroadException
class MapRMfsPlugin(object):
    PLUGIN_NAME = "MapRMFS"

    def __init__(self):
        self.spyglass = None
        self.debug_fp = None

    def config(self, config_obj):
        # Typical plugin initializaion
        # <Plugin python>
        #     ModulePath "/opt/mapr/collectd/collectd-5.8.0/lib/collectd"
        #     LogTraces true
        #     Interactive false
        #     Import "maprmfs"
        #     <Module maprmfs>
        #         spyglass "/opt/mapr/bin/spyglass"
        #         debug_file "/opt/mapr/collectd/collectd-5.8.0/var/log/collectd/collectd_mfs_debug.log"
        #     </Module>
        # </Plugin>
        try:
            self.log_info("Configuring MapR MFS Plugin for Python")

            for node in config_obj.children:
                key = node.key.lower()
                val = node.values[0]

                if key == "spyglass":
                    self.log_info("Spyglass entry found; Processing...")

                    if not os.path.exists(val):
                        self.log_error("Spyglass path does not exist at: " + val)
                        break

                    if not os.path.isfile(val):
                        self.log_error("Spyglass path is not a file: " + val)
                        break

                    if not os.access(val, os.X_OK):
                        self.log_error("Spyglass path is not an executable: " + val)
                        break

                    self.spyglass = val
                    self.log_info("Spyglass executable will be used at path: " + self.spyglass)

                if key == "debug_file":
                    self.log_info("Debug file entry found; Processing...")
                    self.log_info("Debug file entry is: " + val)
                    self.debug_fp = open(val, "w")
                    self.log_info("Debug file opened")
        except Exception, e:
            self.log_error("Configuring MapR MFS Plugin for Python failed", e)

    def init(self):
        try:
            self.log_info("Loading MapR MFS Plugin for Python")
        except Exception, e:
            self.log_error("Loading MapR MFS Plugin for Python failed", e)

    def read(self):
        try:
            self.log_info("Reading MapR MFS Plugin for Python")
            vl = collectd.Values(type='gauge', type_instance='key1')
            vl.plugin = MapRMfsPlugin.PLUGIN_NAME

            sg_output = self.get_spyglass_output()
            self.log_info(sg_output)

            _v = int(random() * 10)
            vl.interval = INTERVAL
            vl.values = [_v]
            self.log_info("Dispatched message: " + str(vl))
            vl.dispatch()
        except Exception, e:
            self.log_error("Reading MapR MFS Plugin for Python failed", e)

    def shutdown(self):
        try:
            self.log_info("Unloading MapR MFS Plugin for Python: " + MapRMfsPlugin.PLUGIN_NAME)
            if self.debug_fp is not None:
                self.debug_fp.close()
                self.debug_fp = None
        except Exception, e:
            self.log_error("Unloading MapR MFS Plugin for Python failed", e)

    def get_spyglass_output(self):
        if self.spyglass is None:
            self.log_error("Spyglass executable is not available to be executed")
            return

        p = subprocess.Popen(self.spyglass, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        (out, err) = p.communicate()
        if p.returncode != 0:
            self.log_error("Spyglass executable failed to be executed: (" + str(p.returncode) + "): " + err)
            return
        return out

    def log_info(self, message):
        if not message:
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": No message given to log")
        else:
            collectd.info(MapRMfsPlugin.PLUGIN_NAME + ": " + message)
            self.log_debug("info", message)

    def log_warning(self, message):
        if not message:
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": No message given to log")
        else:
            collectd.warning(MapRMfsPlugin.PLUGIN_NAME + ": " + message)
            self.log_debug("warning", message)

    def log_error(self, message, exc=None):
        if not message:
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": No message given to log")
        else:
            if exc:
                message = message + "; Exception: " + str(exc)
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": " + message)
            self.log_debug("error", message)

    def log_debug(self, level, message):
        if self.debug_fp is not None:
            dt = datetime.datetime.today().strftime("[%Y-%m-%d %H:%M:%S] ")
            self.debug_fp.write(str(dt) + "[" + level + "] " + MapRMfsPlugin.PLUGIN_NAME + ": " + message + os.linesep)
            self.debug_fp.flush()


mapr_mfs_plugin = MapRMfsPlugin()

collectd.register_config(mapr_mfs_plugin.config)
collectd.register_init(mapr_mfs_plugin.init)
collectd.register_read(mapr_mfs_plugin.read, INTERVAL)
collectd.register_shutdown(mapr_mfs_plugin.shutdown)
# This will enable the plugin to get every write message that collectd is writing (not needed)
# collectd.register_write(write_func)
# collectd.register_flush(flush_func)
# This will enable the plulgin to get every collectd log message (not needed)
# collectd.register_log(log_func)
# collectd.register_notification(notification_func)
