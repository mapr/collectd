#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-`hostname -f`}"
INTERVAL="${COLLECTD_INTERVAL:-600}"
while sleep "$INTERVAL"
do
 sleep $INTERVAL
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
          used=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
          echo "PUTVAL \"$HOSTNAME/volumemetrics.$volumename.$metricname/counter\" interval=$INTERVAL N:$used"
        fi
      done
    fi
  done
done
