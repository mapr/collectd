#!/usr/bin/env python
import datetime
import fcntl
import sys
import time
import math
import os
import socket
import subprocess

import collectd

# The interval that this plugin will get called from CollectD
INTERVAL = 10
# When reading spyglass output, how long to wait to determine that we reached the end of the current stdout buffer
READLINE_THRESHOLD = 2
# The maximum amount of lines to process in one CollectD interval. This should typically be 1 or 2 lines but this is
# a stop-gap measure to make sure the plugin returns
MAX_LINES_PROCESSED = 50
SPYGLASS_END_OF_METRIC = "!!<end_spyglass_statistic>!!\n"

collectd.info("Starting MapR MFS Plugin for Python")


# noinspection PyBroadException
class MapRMfsPlugin(object):
    PLUGIN_NAME = "MapRMFS"
    LOG_INFO = 0
    LOG_WARNING = 1
    LOG_ERROR = 2

    def __init__(self):
        self.spyglass = None
        self.spyglass_process = None
        self.debug_fp = None
        self.debug_file_level = MapRMfsPlugin.LOG_ERROR
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
        #         debug_file_level "warning"
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

                if key == "debug_file_level":
                    self.log_info("Debug file log level entry found; Processing...")
                    self.debug_file_level = MapRMfsPlugin.log_level_str_to_int(val)

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

            self.log_info("Configured MapR MFS Plugin for Python")
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
        except Exception, e:
            self.log_error("Reading MapR MFS Plugin for Python failed executing Spyglass executable", e)
            return

        for line in sg_output:
            sg_list = line.split(" ")
            self.log_info("There are {0} items from the spyglass output".format(str(len(sg_list))))
            for i in range(0, len(sg_list)):
                try:
                    sg_items = sg_list[i].split(":")

                    if len(sg_items) <= 1 or len(sg_list[i].strip()) == 0:
                        # self.log_info("spyglass metric at index {0} is blank". format(i))
                        continue

                    if len(sg_items) != 3 and len(sg_items) != 5:
                        self.log_error("spyglass metric '{0}' has an invalid length of {1}". format(sg_list[i], len(sg_items)))
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

        # start or restart spyglass executable if necessary
        self.restart_spyglass()
        if self.spyglass_process is None:
            self.log_error("Spyglass executable could not be started")
            return None

        output = list()
        lines_processed = 0
        while lines_processed <= MAX_LINES_PROCESSED:
            start_read = time.time()
            line = self.spyglass_process.stdout.readline()
            end_read = time.time()
            time_delta = int(math.ceil(end_read - start_read))
            lines_processed += 1

            self.log_info("spyglass output line retrieved in less than {0} seconds".format(time_delta))
            output.append(line.replace("\n", ""))

            if time_delta >= READLINE_THRESHOLD:
                self.log_info("spyglass output wait time of {0} exceeded threshold of {1}; "
                              "Sending output to CollectD".format(time_delta, READLINE_THRESHOLD))
                break

        if lines_processed > MAX_LINES_PROCESSED:
            self.log_error("The amount of lines processed {0} exceeded the threshold {1}"
                           .format(lines_processed, MAX_LINES_PROCESSED))

        self.log_info("Sending {0} line(s) of spyglass output to CollectD".format(len(output)))
        return output

    # def get_spyglass_output(self):
    #     if self.spyglass is None:
    #         self.log_error("Spyglass executable is not available to be executed")
    #         return None
    #
    #     # start or restart spyglass executable if necessary
    #     self.restart_spyglass()
    #     if self.spyglass_process is None:
    #         self.log_error("Spyglass executable could not be started")
    #         return None
    #
    #     output = list()
    #     lines_processed = 0
    #     while lines_processed <= MAX_LINES_PROCESSED:
    #         self.log_info("Reading line from spyglass...")
    #         try:
    #             line = self.spyglass_process.stdout.read()
    #         except:
    #             self.log_info("End of pipe found. Sending metrics to CollectD")
    #             break
    #
    #         self.log_info("Line read from spyglass")
    #         if line.startswith(SPYGLASS_END_OF_METRIC):
    #             self.log_info("Found end of metric")
    #         else:
    #             self.log_info("Found metric data starting with: {0}".format(line[:20]))
    #             output.append(line.replace("\n", ""))
    #
    #         lines_processed += 1
    #
    #     if lines_processed > MAX_LINES_PROCESSED:
    #         self.log_error("The amount of lines processed {0} exceeded the threshold {1}"
    #                        .format(lines_processed, MAX_LINES_PROCESSED))
    #
    #     self.log_info("Sending {0} line(s) of spyglass output to CollectD".format(len(output)))
    #    return output

    def restart_spyglass(self):
        if self.spyglass_process is not None:
            poll_result = self.spyglass_process.poll()
            # if the spyglass app is terminated
            if poll_result is not None:
                self.log_warning("Spyglass application recently stopped with exit code: {0}".format(poll_result))
                self.spyglass_process = None
            else:
                self.log_info("Spyglass application is still running")

        if self.spyglass_process is None:
            self.log_warning("Spyglass application is not running; Restarting spyglass...")

            interval_param = "--interval:{0}".format(INTERVAL)
            execute = [self.spyglass, interval_param]
            try:
                self.spyglass_process = subprocess.Popen(args=execute, stdout=subprocess.PIPE, shell=False, bufsize=0)

                # TODO: Perhaps find a way to make stdout not blocking so we can collect data without blocking on readline() calls
                # make stdout a non-blocking file
                # fd = sys.stdout.fileno()
                # fl = fcntl.fcntl(fd, fcntl.F_GETFL)
                # fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

                self.log_info("Spyglass application started")
            except OSError, ose:
                self.log_error("OS error executing spyglass application: {0}".format(str(ose)))
                self.spyglass_process = None
            except Exception, e:
                self.log_error("Unexpected error executing spyglass application: {0}".format(str(e)))
                self.spyglass_process = None
                raise e

        # interval_param = "--interval:{0}".format(INTERVAL)
        # execute = [self.spyglass, interval_param]

        # p = subprocess.Popen(execute, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        # (out, err) = p.communicate()
        # if p.returncode == -15:
        #     self.log_warning("Spyglass executable terminated early probably due to CollectD shutting down")
        #     return None
        # if p.returncode != 0:
        #     self.log_error("Spyglass executable failed to be executed: (" + str(p.returncode) + "): " + err)
        #     return None
        #
        # return out

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
        if self.debug_fp is not None and message is not None:
            msg_level = MapRMfsPlugin.log_level_str_to_int(level)
            if msg_level >= self.debug_file_level:
                dt = datetime.datetime.today().strftime("[%Y-%m-%d %H:%M:%S] ")
                self.debug_fp.write(str(dt) + "[" + level + "] " + MapRMfsPlugin.PLUGIN_NAME + ": " + message + os.linesep)
                self.debug_fp.flush()

    @staticmethod
    def log_level_str_to_int(message_level_str):
        if not message_level_str or len(message_level_str) == 0:
            return MapRMfsPlugin.LOG_INFO

        message_level_str = message_level_str.lower()
        if message_level_str == "info":
            return MapRMfsPlugin.LOG_INFO
        if message_level_str == "warning":
            return MapRMfsPlugin.LOG_WARNING
        if message_level_str == "error":
            return MapRMfsPlugin.LOG_ERROR
        return MapRMfsPlugin.LOG_INFO


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
