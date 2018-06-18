#!/bin/bash

# parses output from spyglass executable into collectd information
# you can get information like this:
# resync:bytesreceivedcount:0
# thread:cpu_usage:0:Rpc:0
# the format is [metric_area]:[metric_name]:[value]
# or
# # the format is [metric_area]:[metric_name]:[value]:[plugin_instance]:[type_instance]

HOSTNAME="${COLLECTD_HOSTNAME:-`hostname -f`}"
INTERVAL="10"

while true
do
  VALUE=`/opt/mapr/bin/spyglass --interval:${INTERVAL}`
  arr=(`echo ${VALUE}`);
  arrLen=${#arr[@]}
  for (( i=0; i<${arrLen}; i++ )); do
    IFS=':' read -ra metrics <<< "${arr[$i]}"
    val=${metrics[2]}
    if echo ${val} | grep -E -- '-[0-9]+\.[0-9]+' > /dev/null ; then
      val=0
    fi
    plugin_instance=""
    type_instance=""
    if [ ! -z "${metrics[3]}" -a "${metrics[3]}"!=" " ]; then
      plugin_instance=-${metrics[3]}
    fi
    if [ ! -z "${metrics[4]}" -a "${metrics[4]}"!=" " ]; then
      type_instance=-${metrics[4]}
    fi
    echo "PUTVAL \"$HOSTNAME/mapr.${metrics[0]}$plugin_instance/${metrics[1]}$type_instance\" interval=$INTERVAL N:$val"
  done
done
