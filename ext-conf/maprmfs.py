#!/usr/bin/env python
import os
from random import random
from time import sleep

import collectd

INTERVAL = 10

collectd.info("Loading MapR MFS Plugin for Python")


# noinspection PyBroadException
class MapRMfsPlugin(object):
    PLUGIN_NAME = "MapRMFS"

    def __init__(self):
        self.spyglass = None

    def config(self, config_obj):
        # Typical plugin initializaion
        # <Plugin python>
        #     ModulePath "/opt/mapr/collectd/collectd-5.8.0/lib/collectd"
        #     LogTraces true
        #     Interactive false
        #     Import "maprmfs"
        #     <Module maprmfs>
        #         spyglass "/opt/mapr/bin/spyglass"
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
        except Exception, e:
            self.log_error("Configuring MapR MFS Plugin for Python", e)

    def init(self):
        try:
            self.log_info("Loading MapR MFS Plugin for Python")
        except Exception, e:
            self.log_error("Loading MapR MFS Plugin for Python", e)

    def read(self):
        try:
            self.log_info("Reading MapR MFS Plugin for Python")
            vl = collectd.Values(type='gauge', type_instance='key1')
            vl.plugin = MapRMfsPlugin.PLUGIN_NAME

            _v = int(random() * 3)
            vl.values = [_v]
            vl.dispatch()
        except Exception, e:
            self.log_error("Reading MapR MFS Plugin for Python", e)

    def shutdown(self):
        try:
            with open("/tmp/scottshutdown.txt") as f:
                f.write("Hello")
                f.flush()

            collectd.error("Unloading MapR MFS Plugin for Python: " + MapRMfsPlugin.PLUGIN_NAME)
        except Exception, e:
            self.log_error("Unloading MapR MFS Plugin for Python", e)

    @staticmethod
    def log_info(message):
        if not message:
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": No message given to log")
        else:
            collectd.info(MapRMfsPlugin.PLUGIN_NAME + ": " + message)

    @staticmethod
    def log_warning(message):
        if not message:
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": No message given to log")
        else:
            collectd.warning(MapRMfsPlugin.PLUGIN_NAME + ": " + message)

    @staticmethod
    def log_error(message, exc=None):
        if not message:
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": No message given to log")
        else:
            if exc:
                message = message + "; Exception: " + str(exc)
            collectd.error(MapRMfsPlugin.PLUGIN_NAME + ": " + message)


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
