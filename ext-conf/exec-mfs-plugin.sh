#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-`hostname -f`}"
INTERVAL="10"
while sleep "$INTERVAL"
do
  VALUE=`/opt/mapr/bin/spyglass`
  arr=(`echo ${VALUE}`);
  arrLen=${#arr[@]}
  for (( i=0; i<${arrLen}; i++ )); do
    IFS=':' read -ra metrics <<< "${arr[$i]}"
    val=${metrics[2]}
    if echo ${val} | grep -E -- '-[0-9]+\.[0-9]+' > /dev/null ; then
      val=0
    fi
    echo "PUTVAL \"$HOSTNAME/mapr.${metrics[0]}/${metrics[1]}\" interval=$INTERVAL N:$val"
  done
done
