#!/bin/bash
HOSTNAME="${COLLECTD_HOSTNAME:-`hostname -f`}"
INTERVAL="60"
while sleep "$INTERVAL"
do
  # Run the maprcli command to get the volume list
  maprcli volume list -json | while read line; do

    # Extract the volume names
    if [[ $line == *"volumename"* ]]
    then
      # Parse the name
      volumename=`echo $line | sed 's/.*://' | sed 's/[,"]//g'`
      used=""
      logicalUsed=""
      snapshotused=""
      totalused=""
      quota=""
      topology=""
      entity=""
      # Run the maprcli volume info
      maprcli volume info -name  $volumename -json | while read output; do
        if [[ ( $output == \"logicalUsed* )  ]];
        then
          logicalUsed=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
        elif [[ ( $output == \"used* )  ]];
        then
          used=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
        elif [[ ( $output == \"snapshotused* )  ]];
        then
          snapshotused=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
        elif [[ ( $output == \"totalused* )  ]];
        then
          totalused=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
        elif [[ ( $output == \"quota* )  ]];
        then
          quota=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
        elif [[ ( $output == \"rackpath* ) ]];
        then
          topology=`echo $output | sed 's/.*://' | sed 's/[,"]//g' | sed 's/\//./g'`
        elif [[ ( $output == \"aename* ) ]];
        then
          entity=`echo $output | sed 's/.*://' | sed 's/[,"]//g'`
        fi
        if [[ -n "$logicalUsed" && -n "$used" && -n "$snapshotused" && -n "$totalused" && -n  "$quota" && -n "$topology"  && -n "$entity" ]]
        then
          # Collect metrics per volume
          echo "PUTVAL \"$HOSTNAME/mapr.volume-${topology:1}/logical_used-$volumename\" interval=$INTERVAL N:$logicalUsed"
          echo "PUTVAL \"$HOSTNAME/mapr.volume-${topology:1}/used-$volumename\" interval=$INTERVAL N:$used"
          echo "PUTVAL \"$HOSTNAME/mapr.volume-${topology:1}/snapshot_used-$volumename\" interval=$INTERVAL N:$snapshotused"
          echo "PUTVAL \"$HOSTNAME/mapr.volume-${topology:1}/total_used-$volumename\" interval=$INTERVAL N:$totalused"
          echo "PUTVAL \"$HOSTNAME/mapr.volume-${topology:1}/quota-$volumename\" interval=$INTERVAL N:$quota"
          # Tag by entity
          echo "PUTVAL \"$HOSTNAME/mapr.volume_by_entity/logical_used-$entity\" interval=$INTERVAL N:$logicalUsed"
          echo "PUTVAL \"$HOSTNAME/mapr.volume_by_entity/used-$entity\" interval=$INTERVAL N:$used"
          echo "PUTVAL \"$HOSTNAME/mapr.volume_by_entity/snapshot_used-$entity\" interval=$INTERVAL N:$snapshotused"
          echo "PUTVAL \"$HOSTNAME/mapr.volume_by_entity/total_used-$entity\" interval=$INTERVAL N:$totalused"
          echo "PUTVAL \"$HOSTNAME/mapr.volume_by_entity/quota-$entity\" interval=$INTERVAL N:$quota"
          break
        fi
      done
    fi
  done
done
