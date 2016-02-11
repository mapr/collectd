#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-`hostname -f`}"
INTERVAL="${COLLECTD_INTERVAL:-6000}"
while sleep "$INTERVAL"
do
  # Run the maprcli command to get the volume list
  maprcli volume list -json | while read line; do

    # Extract the volume names
    if [[ $line == *"volumename"* ]]
    then
      # Parse the name
      volumename=`echo $line | sed 's/.*://' | sed 's/[,"]//g'`
      # Run the maprcli volume info
      maprcli volume info -name  $volumename -json | while read output; do
        if [[ ( $output == *"used"* ) || ( $output == *"Used"* ) ]];
        then
          metricname=`echo $output | sed 's/:.*//' | sed 's/[,"]//g'`
          metricvalue=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
        elif [[ ( $output == *"rackpath"* ) ]];
        then
          topology=`echo $output | sed 's/.*://' | sed 's/[,"]//g' | sed 's/\//./g'`
        fi
        # Collect metrics per volume
        echo "PUTVAL \"$HOSTNAME/mapr.volume/$metricname-$volumename\" interval=$INTERVAL N:$metricvalue"
        # Collect metrics per topology
        echo "PUTVAL \"$HOSTNAME/mapr.volume-${topology:1}/$metricname\" interval=$INTERVAL N:$metricvalue"
      done
    fi
  done
done
