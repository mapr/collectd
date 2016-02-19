#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-`hostname -f`}"
INTERVAL="3000"
while sleep "$INTERVAL"
do
  VALUE=`/opt/mapr/bin/spyglass`
  arr=(`echo ${VALUE}`);
  arrLen=${#arr[@]}
  for (( i=0; i<${arrLen}; i++ )); do
    IFS=':' read -ra metrics <<< "${arr[$i]}"
    echo "PUTVAL \"$HOSTNAME/mapr.${metrics[0]}/${metrics[1]}\" interval=$INTERVAL N:${metrics[2]}"
  done
done
