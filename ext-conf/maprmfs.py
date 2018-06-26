#!/usr/bin/env python
import datetime
import os
import socket
import subprocess

import collectd

INTERVAL = 10

collectd.info("Starting MapR MFS Plugin for Python")


# noinspection PyBroadException
class MapRMfsPlugin(object):
    PLUGIN_NAME = "MapRMFS"

    def __init__(self):
        self.spyglass = None
        self.debug_fp = None
        self.fqdn = None
        # These are datasets that are sent in from spyglass that should not be sent to out to OT.
        self.ignored_datasets_file = None
        self.ignored_datasets_list = None

    def config(self, config_obj):
        # Typical plugin initializaion
        # <Plugin python>
        #     ModulePath "/opt/mapr/collectd/collectd-5.8.0/lib/collectd"
        #     LogTraces true
        #     Interactive false
        #     Import "maprmfs"
        #     <Module maprmfs>
        #         debug_file "/opt/mapr/collectd/collectd-5.8.0/var/log/collectd/collectd_mfs_debug.log"
        #         spyglass "/opt/mapr/bin/spyglass"
        #         ignored_datasets_file "/opt/mapr/collectd/collectd-5.8.0/lib/collectd/maprmfs_ignored_datasets.txt"
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
                        continue
                    if not os.path.isfile(val):
                        self.log_error("Spyglass path is not a file: " + val)
                        continue
                    if not os.access(val, os.X_OK):
                        self.log_error("Spyglass path is not an executable: " + val)
                        continue

                    self.spyglass = val
                    self.log_info("Spyglass executable will be used at path: " + self.spyglass)

                if key == "debug_file":
                    self.log_info("Debug file entry found; Processing...")
                    self.log_info("Debug file entry is: " + val)
                    self.debug_fp = open(val, "w")
                    self.log_info("Debug file opened at: {0}".format(val))

                if key == "ignored_datasets_file":
                    self.log_info("Ignored datasets file entry found; Processing...")
                    if not os.path.exists(val):
                        self.log_error("Ignored datasets path does not exist at: " + val)
                        continue
                    if not os.path.isfile(val):
                        self.log_error("Ignored datasets path is not a file: " + val)
                        continue

                    self.ignored_datasets_file = val
                    self.log_info("Ignored datasets path is: {0}".format(val))

        except Exception, e:
            self.log_error("Configuring MapR MFS Plugin for Python failed", e)

    def init(self):
        try:
            self.log_info("Loading MapR MFS Plugin for Python")
            self.fqdn = socket.getfqdn()
            self.log_info("FQDN is {0}".format(self.fqdn))

            if self.ignored_datasets_file is not None:
                with open(self.ignored_datasets_file, "r") as fp:
                    ignored_datasets = fp.readlines()

                # remove newlines at the end of each dataset
                for i in range(0, len(ignored_datasets)):
                    ignored_datasets[i] = ignored_datasets[i].rstrip(os.linesep)

                if not ignored_datasets or len(ignored_datasets) == 0:
                    self.log_warning("An ignored dataset file was specified but no datasets were found in the file")
                else:
                    self.ignored_datasets_list = ignored_datasets
                    self.log_info("There were {0} ignored datasets specified".format(len(self.ignored_datasets_list)))
        except Exception, e:
            self.log_error("Loading MapR MFS Plugin for Python failed", e)

    def read(self):
        self.log_info("Reading MapR MFS Plugin for Python")

        try:
            sg_output = self.get_spyglass_output()
            if sg_output is None or len(sg_output) == 0:
                self.log_warning("No spyglass information returned")
                return

            sg_list = sg_output.split(" ")
            self.log_info("There are {0} items from the spyglass output".format(str(len(sg_list))))
        except Exception, e:
            self.log_error("Reading MapR MFS Plugin for Python failed executing Spyglass executable", e)
            return

        for i in range(0, len(sg_list)):
            try:
                sg_items = sg_list[i].split(":")

                if len(sg_items) <= 1 or len(sg_list[i].strip()) == 0:
                    # self.log_info("spyglass metric at index {0} is blank". format(i))
                    continue

                if len(sg_items) != 2 and len(sg_items) != 3 and len(sg_items) != 5:
                    self.log_error("spyglass metric '{0}' has an invalid length of {1}". format(sg_list[i], len(sg_items)))
                    continue

                if len(sg_items) == 2:
                    # TODO: we need values from Spyglass coming back with 3 or 5 values not 2 that the db metrics come back as
                    continue

                if self.ignored_datasets_list and sg_items[1] in self.ignored_datasets_list:
                    continue

                metric_value = sg_items[2]
                try:
                    float(metric_value)
                except ValueError:
                    metric_value = 0.0

                values = collectd.Values()
                values.host = self.fqdn
                values.plugin = "mapr.{0}".format(sg_items[0])
                values.type = sg_items[1]
                values.interval = INTERVAL
                values.values = [metric_value]
                if len(sg_items) > 3:
                    values.plugin_instance = sg_items[3]
                    values.type_instance = sg_items[4]

                values.dispatch()
            except Exception, e:
                self.log_error("Could not process spyglass output '{0}' at index {1}".format(sg_list[i], i), e)

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
            return None

        p = subprocess.Popen(self.spyglass, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        (out, err) = p.communicate()
        if p.returncode != 0:
            self.log_error("Spyglass executable failed to be executed: (" + str(p.returncode) + "): " + err)
            return None

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
