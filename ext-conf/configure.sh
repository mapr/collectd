#!/bin/bash
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved

#############################################################################
#
# Configure script for collectd
#
# configures the mapr streams, opentsdb and jmx facilities
# enables/disables the mapr streams, opentsdb and jmx facilities
#
# TODO: add support to tweak other collection facilities, like disk, fs, network
# TODO: Need to add support to clean up old copies
#
# __INSTALL_ (two _ at the end) gets expanded to __INSTALL__ during pakcaging
# set COLLECTD_HOME explicitly if running this in a source built env.
#
# This script is sourced by the master configure.sh to setup collectd during
# install. If it is run with command line arguments, it is assumed to be
# run in a standalone fashion and will override the following variables that
# it normally inherits from the master configure.sh
#
#  otNodesCount - count of opentTSDB servers
#  otNodesList  - list of opentTSDB servers
#  otPort       - port number the openTSDB servers listen on
#  zkNodesCount - count of zookeeper servers
#  zkNodesList  - list of zookeeper servers
#  zkClientPort - port number the zookeeper servers listen on
#
#  MAPR_HOME    - directory where the packages are installed
#  MAPR_USER    - user name for the MAPR user
#############################################################################

COLLECTD_HOME="${COLLECTD_HOME:-__INSTALL__}"
CD_CONF_FILE="${CD_CONF_FILE:-${COLLECTD_HOME}/etc/collectd.conf}"
NEW_CD_CONF_FILE="${NEW_CD_CONF_FILE:-${COLLECTD_HOME}/etc/collectd.conf.progress}"
CD_CONF_FILE_SAVE_AGE="30"
AWKLIBPATH="${AWKLIBPATH:-$COLLECTD_HOME/lib/awk}"
CD_NOW=`date "+%Y%m%d_%H%M%S"`
RM_REST_PORT=8088
RM_SECURE_REST_PORT=8090
OOZIE_REST_PORT=11000
OOZIE_SECURE_REST_PORT=11443
RM_JMX_PORT=8025
NM_JMX_PORT=8027
CLDB_JMX_PORT=7220
DRILLBITS_JMX_PORT=6090
OOZIE_JMX_PORT=9010
HBASE_MASTER_JMX_PORT=10101
HBASE_REGION_SERVER_JMX_PORT=10102
JMX_INSERT='-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.password.file=$MAPR_HOME/conf/jmxremote.password -Dcom.sun.management.jmxremote.access.file=$MAPR_HOME/conf/jmxremote.access -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port'
HADOOP_JMX_INSERT="#Enable JMX for MaprMonitoring\nJMX_OPTS=\"${JMX_INSERT//\$MAPR_HOME/$MAPR_HOME}\""
OOZIE_JMX_INSERT="${JMX_INSERT/jmxremote.authenticate=false/jmxremote.authenticate=true}=$OOZIE_JMX_PORT\""
YARN_JMX_RM_OPT_STR='$JMX_OPTS='${RM_JMX_PORT}
YARN_JMX_NM_OPT_STR='$JMX_OPTS='${NM_JMX_PORT}
MAPR_HOME=${MAPR_HOME:-/opt/mapr}
COLLECTD_CUSTOM_CONF_DIR="${MAPR_HOME}/collectd/conf"
CLUSTER_ID_FILE="${MAPR_HOME}/conf/clusterid"
HADOOP_VER=$(cat "$MAPR_HOME/hadoop/hadoopversion")
YARN_BIN="${MAPR_HOME}/hadoop/hadoop-${HADOOP_VER}/bin/yarn"
CD_CONF_ASSUME_RUNNING_CORE=${isOnlyRoles:-0}
CD_NM_ROLE=0
CD_CLDB_ROLE=0
CD_RM_ROLE=0
CD_OOZIE_ROLE=0
CD_OT_ROLE=0
CD_HBASE_REGION_SERVER_ROLE=0
CD_HBASE_MASTER_ROLE=0
CD_DRILLBITS_ROLE=0
CLDB_RUNNING=0
CLDB_RETRIES=12
CLDB_RETRY_DLY=5
CD_ENABLE_SERVICE=0
CD_RESTART_SVC_LIST=""
nodecount=0
nodelist=""
nodeport=4242
secureCluster=0
useStreams=1

if [ -e "${MAPR_HOME}/server/common-ecosystem.sh" ]; then
    . "${MAPR_HOME}/server/common-ecosystem.sh"
else
   echo "Failed to source common-ecosystem.sh"
   exit 0
fi

#TODO 
# try to discover jmx ports for services

#############################################################################
# function to adjust ownership
#############################################################################
function adjustOwnership() {
    if [ -f "/etc/logrotate.d/collectd" ]; then
        if [ "$MAPR_USER" != "mapr" -o "$MAPR_GROUP" != "mapr" ]; then
            sed -i -e 's/create 640 mapr mapr/create 640 '"$MAPR_USER $MAPR_GROUP/" /etc/logrotate.d/collectd
        fi
    fi
    # set correct user/group on Exec plugins
    sed -i -e 's/\(#* *Exec *\)"[a-zA-Z0-9]*:[a-zA-Z0-9]*"/\1 "'"$MAPR_USER:$MAPR_GROUP\"/" ${NEW_CD_CONF_FILE}
    # in case we could not do it from the package
    chown -R "$MAPR_USER":"$MAPR_GROUP" $COLLECTD_HOME
 }

#############################################################################
# Function to uncomment a section
# $1 is the sectionTag prefix we will use to determine section to uncomment
#############################################################################
function enableSection()
{
    # $1 is the sectionTag prefix we will use to determine section to uncomment
    awk -f ${AWKLIBPATH}/uncommentSection.awk -v tag="$1" \
        ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi
}

#############################################################################
# Function to comment a section if it isn't already
# $1 is the sectionTag prefix we will use to determine section to comment out
#############################################################################
function disableSection()
{
    # $1 is the sectionTag prefix we will use to determine section to uncomment
    awk -f ${AWKLIBPATH}/commentOutSection.awk -v tag="$1" \
        ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi
}

#############################################################################
# Function to comment out a section
# $1 is the sectionTag prefix we will use to determine section to comment out
#############################################################################
function disableSection()
{
    # $1 is the sectionTag prefix we will use to determine section to comment out
    awk -f ${AWKLIBPATH}/commentOutSection.awk -v tag="$1" \
        ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi
}

#############################################################################
# Function to remove a section
#
# $1 is the sectionTag prefix we will use to determine section to remove
#############################################################################
function removeSection()
{
    awk -f ${AWKLIBPATH}/removeSection.awk -v tag="$1" \
        ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi
}
#############################################################################
# Function to fill a section
#
# $1 is the sectionTag prefix we will use to determine section to replace
# $2 is the file containing the new content of the section
#############################################################################
function fillSection()
{
    removeSection $1
    awk -f ${AWKLIBPATH}/replaceSection.awk -v tag="$1" -v newSectionContentFile="$2" \
        ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi
}

#############################################################################
# Function to configure a ServiceURL within a section
#
# $1 is the sectionTag prefix we will use to determine section to uncomment
# $2 is the hostname to use in the serviceURL
# $3 is a flag indiciating if this is a http or jmx url
# $4 is a flag indiciating if this needs to be a secure url
# $5 is the port to use
#############################################################################
function configureServiceURL()
{
    # $1 is the sectionTag prefix we will use to determine section to uncomment
    local findPattern
    local replacePattern
    local hostname
    local urlType
    local secure
    local secureStr
    local oldPort
    local section
    local password
    local user

    section="$1"
    hostname="$2"
    urlType=$3
    secure="$4"
    secureStr=""
    port="$5"
    password=""
    user=""

    if [ $secure -eq 1 ]; then
        secureStr="s"
    fi
    if [ "$urlType" == "jmx" ]; then
        findPattern="ServiceURL \"service:jmx:rmi:///jndi/rmi://.*:[0-9]+/"
        replacePattern="ServiceURL \"service:jmx:rmi:///jndi/rmi://$hostname:$port/"
    else
        findPattern="ServiceURL \"http://.*:[0-9]+/"
        replacePattern="ServiceURL \"http$secureStr://$hostname:$port/"
    fi
    awk -f ${AWKLIBPATH}/substituteWithinSection.awk -v tag="$1" -v findPattern="$findPattern" \
        -v replacePattern="$replacePattern" ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi
    if [ "$urlType" == "jmx" ]; then
        # we always remove it so we don't have dups and if someone updates the 
        # jmx password file we will get the new values
        awk -f ${AWKLIBPATH}/removeJmxLoginDetail.awk -v tag="$1" ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
        if [[ $? -eq 0 ]]; then
            mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
        fi
        if [ $secure -eq 1 ]; then

            if [ -f "$JMX_REMOTE_PASSWORD_FILE" ]; then
                password=$(fgrep $MAPR_USER $JMX_REMOTE_PASSWORD_FILE | cut -d' ' -f2)
            else
                logWarn "collectd - no jmx remote password file found"
            fi
            awk -f ${AWKLIBPATH}/addJmxLoginDetail.awk -v tag="$1" -v user="$MAPR_USER" -v password="$password" \
                ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
            if [[ $? -eq 0 ]]; then
                mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
            fi
        fi
    fi
}

#############################################################################
# Function to configure Hostname
#
# by default if we don't set the Hostname, collectd will use
# determine it using the gethostname(2) system call.
#############################################################################
function configureHostname() {
    # Changes this global
    # #Hostname    "localhost"
    local host_name
    host_name=$(hostname -f)
    if [ -z "$host_name" ]; then
        host_name=$(hostname) # some aws machine reports an empty string with hostname -f
    fi
    sed -i -e 's/#Hostname.*$/Hostname' ${host_name}/ ${NEW_CD_CONF_FILE}
}

#############################################################################
# Function to figure out what roles this node has
#
# sets globals
# CD_NM_ROLE
# CD_CLDB_ROLE
# CD_RM_ROLE
# CD_HBASE_REGION_SERVER_ROLE
# CD_HBASE_MASTER_ROLE
# CD_DRILLBITS_ROLE
# CD_OOZIE_ROLE
# CD_OT_ROLE
#############################################################################
function getRoles() {
    hasRole 'resourcemanager' && CD_RM_ROLE=1
    hasRole 'nodemanager' && CD_NM_ROLE=1
    hasRole 'cldb' ] && CD_CLDB_ROLE=1
    hasRole 'hbregionserver '] && CD_HBASE_REGION_SERVER_ROLE=1
    hasRole 'hbmaster' ] && CD_HBASE_MASTER_ROLE=1
    hasRole 'drill-bits' ] && CD_DRILLBITS_ROLE=1
    hasRole 'oozie' && CD_OOZIE_ROLE=1
    hasRole 'opentsdb' && CD_OT_ROLE=1
}

#############################################################################
# Function to configure Interface Plugin
#
# If no configuration if given, the traffic-plugin will collect data
# from all interfaces
#
#############################################################################
function configureInterfacePlugin() {
    # Changes this plugin
    # <Plugin interface>
    #     Interface "eth0"
    #     IgnoreSelected true
    # </Plugin>
    local loopifs
    loopifs=$(ls /dev/loop[0-9]*)
    awk '/<Plugin interface>/ { next;next; print \tInterface \"${loopifs}\"; \
        print \tIngoreSelection true' ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi
}


#############################################################################
# Function to configure Disk Plugin
#
function configureDiskPlugin()
{
    :
}


#############################################################################
# Function to configure Zookeeper Plugin
#
# This function uses the following globals from master configure.sh
# zkNodesList
# zkClientPort
#
#############################################################################
function configureZookeeperConfig() {
    # Changes this plugin
    #<Plugin zookeeper>
    #    Host "localhost"
    #    Port "2181"
    #</Plugin>

    # Need to potentially uncomment
    #
    local zoohost
    zoohost=${zkNodesList%%,*}
    zoohost=${zoohost%%:*}
    awk -v hostname=$zoohost -v port=$zkClientPort -v plugin=zookeeper \
        -f ${AWKLIBPATH}/condfigurePlugin.awk ${NEW_CD_CONF_FILE} > ${NEW_CONFIG_FILE}.tmp
    if [[ $? -eq 0 ]]; then
        mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
    fi

}

#############################################################################
# Function to enable plugin
#
# $1 is the name of the plugin
#############################################################################
function pluginEnable() {
    sed -i -e "s/#LoadPlugin $1/LoadPlugin $1/;" ${NEW_CD_CONF_FILE}
}

#############################################################################
# Function to disable plugin
#
# $1 is the name of the plugin
#############################################################################
function pluginDisable() {
    sed -i -e "s/\(^LoadPlugin $1\)/#\1/;" ${NEW_CD_CONF_FILE}
}

#############################################################################
# Function to configure mapr streams plugin
#############################################################################
function configuremaprstreamsplugin()
{ 
    if [ $useStreams -eq 1 ]; then
        # first enable the plugin   
        pluginEnable write_maprstreams

        # configure maprstreams
        # <plugin write_maprstreams>
        #     <node>
        # 		Path "/var/mapr/mapr.monitoring/streams"
        #           HostTags "clusterid=$clusterId clustername=$clusterName"
        #     </Node>
        # </Plugin>
        enableSection MAPR_CONF_STREAMS_TAG
    else
        pluginDisable write_maprstreams
        disableSection MAPR_CONF_STREAMS_TAG
    fi
    return 0
}

#############################################################################
# Function to configure opentsdb plugin
#
# uses nodeslist and nodeport arguments
#############################################################################
function configureopentsdbplugin()
{
    if [ $useStreams -eq 0 ]; then
        # first enable the plugin

        pluginEnable write_tsdb

        # configure opentsdb connections
        # <plugin write_tsdb>
        #     <node>
        #             host "spy-98.qa.lab"
        #             port "4242"
        #             storerates false
        #             AlwaysAppendDS false
        #     </Node>
        # </Plugin>
        local tsdbhost
        local nodesList=""
        for i in $(echo $nodelist | sed "s/,/ /g"); do
            tsdbhost=${i%%:*}
            nodesList=$nodesList","$tsdbhost
        done
        nodesList=${nodesList:1}
        enableSection MAPR_CONF_OT_TAG
        awk -v hostname=$nodesList -v port=$nodeport -v plugin=write_tsdb \
            -f ${AWKLIBPATH}/configurePlugin.awk ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.tmp
        if [[ $? -eq 0 ]]; then
            mv ${NEW_CD_CONF_FILE}.tmp ${NEW_CD_CONF_FILE}
        fi
    else
        pluginDisable write_tsdb
        disableSection MAPR_CONF_OT_TAG
    fi

    return 0
}

#############################################################################
# Function to configure java jmx plugin
#
#############################################################################
function configurejavajmxplugin()
{
    # use the roles file to determine if this node is a Resource Manager

    # configure resource manager rest Plugin
    # <plugin "restplugin">
    #  <service "resourcemanager_Rest">
    #  </service>
    #  <connection>
    #       service "resourceManager_Rest"
    #       serviceurl "http://RESOURCEMANAGER_IP:8088/ws/v1/cluster/scheduler/"
    #  </connection>
    # </plugin>

    # configure jmx connections
    #
    #    <connection>
    #     <xxx this port is fixed"
    #   serviceurl "service:jmx:rmi:///jndi/rmi://CLDB_IP:7220/jmxrmi"
    #   collect "cldbserver"
    # </connection>
    #
    #    <connection>
    #     <xxx this port is nOT FIXED yet"
    #      serviceurl "service:jmx:rmi:///jndi/rmi://RESOURCEMANAGER_IP:8025/jmxrmi"
    #      #includeportinhostname true
    #      collect "queuemetrics"
    #    </connection>
    #
    #    <connection>
    #     <xxx this port is nOT FIXED yet"
    #           serviceurl "service:jmx:rmi:///jndi/rmi://NODEMANAGER_IP:8027/jmxrmi"
    #           #includeportinHostname true
    #           collect "nodeManagerMetrics"
    #     </connection>
    #

    # XXX potential problem with multi-nic nodes
    host_name=$(hostname -f)
    if [ -z "$host_name" ]; then
        host_name=$(hostname) # some aws machine reports an empty string with hostname -f
    fi
    if [ ${CD_RM_ROLE} -eq 1  -o ${CD_NM_ROLE} -eq 1  -o ${CD_CLDB_ROLE} -eq 1 -o\
         ${CD_HBASE_MASTER_ROLE} -eq 1 -o ${CD_HBASE_REGION_SERVER_ROLE} -eq 1 -o ${CD_DRILLBITS_ROLE} -eq 1 ]; then
        enableSection MAPR_CONF_JMX_TAG
        sed -i 's@${fastjmx_prefix}@'$COLLECTD_HOME'@g' ${NEW_CD_CONF_FILE}
        if [ ${CD_RM_ROLE} -eq 1 -o ${CD_OOZIE_ROLE} -eq 1 -o ${CD_OT_ROLE} -eq 1 ]; then
            enableSection MAPR_CONF_REST_TAG
            if [ ${CD_RM_ROLE} -eq 1 ]; then
                enableSection MAPR_CONF_RM_REST_TAG
                if [ $secureCluster -eq 1 ]; then
                    configureServiceURL MAPR_CONF_RM_REST_TAG $host_name https $secureCluster $RM_SECURE_REST_PORT
                else
                    configureServiceURL MAPR_CONF_RM_REST_TAG $host_name http $secureCluster $RM_REST_PORT
                fi
            fi
            if [ ${CD_OOZIE_ROLE} -eq 1 ]; then
                enableSection MAPR_CONF_OOZIE_REST_TAG
                if [ $secureCluster -eq 1 ]; then
                    configureServiceURL MAPR_CONF_OOZIE_REST_TAG $host_name https $secureCluster $OOZIE_SECURE_REST_PORT
                else
                    configureServiceURL MAPR_CONF_OOZIE_REST_TAG $host_name http $secureCluster $OOZIE_REST_PORT
                fi
            fi
            ## TODO - Don't enable this by default - Determine later if these metrics are needed 
            #if [ ${CD_OT_ROLE} -eq 1 ]; then
            #    enableSection MAPR_CONF_OPENTSDB_REST_TAG
            #    if [ $secureCluster -eq 1 ]; then
            #        configureServiceURL MAPR_CONF_OPENTSDB_REST_TAG $host_name http $secureCluster $nodeport
            #    else
            #        configureServiceURL MAPR_CONF_OPENTSDB_REST_TAG $host_name http $secureCluster $nodeport
            #    fi
            #fi
        fi
        configureConnections
    fi
}


#############################################################################
# Function to configure connections
#
# uses global CLDB_ROLE, CD_RM_ROLE, CD_NM_ROLE
#############################################################################
function configureConnections() {
    local host_name
    # XXX potential problem with multi-nic nodes
    host_name=$(hostname -f)
    if [ -z "$host_name" ]; then
        host_name=$(hostname) # some aws machine reports an empty string with hostname -f
    fi
    if [ ${CD_CLDB_ROLE} -eq 1 ]; then
        enableSection MAPR_CONN_CONF_CLDB_TAG
        enableSection MAPR_CONF_CLDB_ALARMS_TAG
        enableSection MAPR_CONF_VOLUMES_TAG
        enableSection MAPR_CONF_TOPOLOGIES_TAG
        configureServiceURL MAPR_CONN_CONF_CLDB_TAG $host_name jmx $secureCluster $CLDB_JMX_PORT
    fi
    if [ ${CD_NM_ROLE} -eq 1 ]; then
        enableSection MAPR_CONN_CONF_NM_TAG
        configureServiceURL MAPR_CONN_CONF_NM_TAG $host_name jmx $secureCluster $NM_JMX_PORT
    fi
    if [ ${CD_RM_ROLE} -eq 1 ]; then
        enableSection MAPR_CONN_CONF_RM_TAG
        configureServiceURL MAPR_CONN_CONF_RM_TAG $host_name jmx $secureCluster $RM_JMX_PORT
    fi
    if [ ${CD_HBASE_MASTER_ROLE} -eq 1 ]; then
        enableSection MAPR_CONN_CONF_HBASE_MASTER_TAG
        configureServiceURL MAPR_CONN_CONF_HBASE_MASTER_TAG $host_name jmx $secureCluster $HBASE_MASTER_JMX_PORT
    fi
    if [ ${CD_HBASE_REGION_SERVER_ROLE} -eq 1 ]; then
        enableSection MAPR_CONN_CONF_HBASE_REGION_SERVER_TAG
        configureServiceURL MAPR_CONN_CONF_HBASE_REGION_SERVER_TAG $host_name jmx $secureCluster $HBASE_REGION_SERVER_JMX_PORT
    fi
    if [ ${CD_DRILLBITS_ROLE} -eq 1 ]; then
        enableSection MAPR_CONN_CONF_DRILLBITS_TAG
        configureServiceURL MAPR_CONN_CONF_DRILLBITS_TAG $host_name jmx $secureCluster $DRILLBITS_JMX_PORT
    fi
    if [ ${CD_OOZIE_ROLE} -eq 1 ]; then
        enableSection MAPR_CONN_CONF_OOZIE_TAG
        configureServiceURL MAPR_CONN_CONF_OOZIE_TAG $host_name jmx $secureCluster $OOZIE_JMX_PORT
    fi
}


#############################################################################
# Function to create link for fast JMX jar
#
# uses global MAPR_HOME
#############################################################################
function createFastJMXLink() {

    local jmx_jar
    # XXX WIll the release jar have SNAPSHOT in it??
    jmx_jar=$(find ${MAPR_HOME}/collectd-fast-jmx -name 'fast*SNAPSHOT.jar')
    if [ -n "${jmx_jar}" ]; then
        if [ ! -h ${COLLECTD_HOME}/lib/fast-jmx-1.1-SNAPSHOT.jar ]; then
            ln -s ${jmx_jar} ${COLLECTD_HOME}/lib/fast-jmx-1.1-SNAPSHOT.jar
        fi
    fi
}


#############################################################################
# Function to configure Hadoop JMX
#
# uses global CLDB_RUNNING, CD_RM_ROLE, CD_NM_ROLE
#############################################################################
function configureHadoopJMX() {
    local rc1
    local rc2
    local jmxAlreadyEnabled=0
    local jmxSecured=0
    local securityChanged=0
    # Enable JMX for RM and NM only if they are installed
    if [ ${CD_RM_ROLE} -eq 1 -o ${CD_NM_ROLE} -eq 1 ]; then
        if grep "^#Enable JMX for MaprMonitoring" ${YARN_BIN} > /dev/null 2>&1; then
            jmxAlreadyEnabled=1
        fi
        if grep "^JMX_OPTS=" ${YARN_BIN} | grep "jmxremote.authenticate=true"> /dev/null 2>&1; then
            jmxSecured=1
        fi
        if [ \( ${secureCluster} -eq 0 -a $jmxSecured -eq 1 \) -o \( ${secureCluster} -eq 1 -a $jmxSecured -eq 0 \) ]; then
            securityChanged=1
        fi

        # only change the script once or when security config changed
        if [ $jmxAlreadyEnabled -eq 0 -o \( $jmxAlreadyEnabled -eq 1 -a $securityChanged -eq 1 \) ]; then
            # save backup copy fist time only
            if [ $jmxAlreadyEnabled -eq 0 ]; then
                cp -p ${YARN_BIN} ${YARN_BIN}.prejmx
            fi
            if [ ${secureCluster} -eq 1 ]; then
                if [ -f ${MAPR_HOME}/conf/jmxremote.password -a -f ${MAPR_HOME}/conf/jmxremote.access ]; then
                    JMX_INSERT_STRING=${HADOOP_JMX_INSERT/.jmxremote.authenticate=false/.jmxremote.authenticate=true}
                else
                    JMX_INSERT_STRING=$HADOOP_JMX_INSERT
                    logWarn "WARNING: Failed to enable secure jmx for NM/RM - see ${YARN_BIN}.tmp.tmp"
                fi
            else
                JMX_INSERT_STRING=$HADOOP_JMX_INSERT
            fi
            if [ $jmxAlreadyEnabled -eq 0 ]; then
                awk -v jmx_ins_after='JAVA_HEAP_MAX' -v jmx_insert="$JMX_INSERT_STRING" \
                    -v jmx_opts_pattern='"\\$COMMAND" = "resourcemanager"' \
                    -v yarn_opts="$YARN_JMX_RM_OPT_STR" \
                    -f ${AWKLIBPATH}/configureYarnJmx.awk ${YARN_BIN} > ${YARN_BIN}.tmp
                rc1=$?
        
                awk  -v jmx_opts_pattern='"\\$COMMAND" = "nodemanager"' \
                     -v yarn_opts="$YARN_JMX_NM_OPT_STR" \
                     -f ${AWKLIBPATH}/configureYarnJmx.awk ${YARN_BIN}.tmp > ${YARN_BIN}.tmp.tmp
                rc2=$?
                if [ $rc1 -eq 0 -a $rc2 -eq 0 ]; then
                    mv ${YARN_BIN}.tmp.tmp ${YARN_BIN}
                    chmod a+x ${YARN_BIN}
                    if [ ${CD_RM_ROLE} -eq 1 ]; then
                        CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST resourcemanager"
                    fi
                    if [ ${CD_NM_ROLE} -eq 1 ]; then
                        CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST nodemanager"
                    fi
                else
                    logWarn "Failed to enable jmx for NM/RM - see ${YARN_BIN}.tmp.tmp"
                fi
                rm -f ${YARN_BIN}.tmp
            else
                if [ $jmxSecured -eq 1 ]; then
                    cur_auth_val="true"
                    new_auth_val="false"
                else
                    cur_auth_val="false"
                    new_auth_val="true"
                fi
                sed -i -e "s/\.jmxremote\.authenticate=$cur_auth_val/\.jmxremote\.authenticate=$new_auth_val/" ${YARN_BIN}
                if [ ${CD_RM_ROLE} -eq 1 ]; then
                    CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST resourcemanager"
                fi
                if [ ${CD_NM_ROLE} -eq 1 ]; then
                    CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST nodemanager"
                fi
            fi
        fi
    fi
}

#############################################################################
# Function to configure HBase JMX
#
# uses global CLDB_RUNNING, CD_HBASE_MASTER_ROLE, CD_HBASE_REGION_SERVER_ROLE
#############################################################################
function configureHbaseJMX() {
    local rc1
    local HBASE_VER
    local HBASE_ENV

    # Enable JMX for HBase Master and HBase Region server only if they are installed
    if [ ${CD_HBASE_MASTER_ROLE} -eq 1 -o ${CD_HBASE_REGION_SERVER_ROLE} -eq 1 ]; then
        # only change the script once
        HBASE_VER=$(cat "$MAPR_HOME/hbase/hbaseversion")
        HBASE_ENV="${MAPR_HOME}/hbase/hbase-${HBASE_VER}/conf/hbase-env.sh"
        if ! grep "^#Enable JMX for MaprMonitoring" ${HBASE_ENV} > /dev/null 2>&1; then
            cp -p ${HBASE_ENV} ${HBASE_ENV}.prejmx
    
            awk -v jmx_uncomment_start='# export HBASE_JMX_BASE=' \
                -v jmx_uncomment_end='# export HBASE_REST_OPTS=' \
                -f ${AWKLIBPATH}/configureHbaseJmx.awk ${HBASE_ENV} > ${HBASE_ENV}.tmp
            rc1=$?
            if [ $rc1 -eq 0 ]; then
                mv ${HBASE_ENV}.tmp ${HBASE_ENV}
                chmod a+x ${HBASE_ENV}
                if [ ${CD_HBASE_MASTER_ROLE} -eq 1 ]; then
                    CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST hbmaster"
                fi
                if [ ${CD_HBASE_REGION_SERVER_ROLE} -eq 1 ]; then
                    CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST hbregionserver"
                fi
            else
                logWarn "Failed to enable jmx for HBase Maser/Region Server - see ${HBASE_ENV}.tmp"
            fi
        fi
    fi
}

#############################################################################
# Function to configure Drill JMX
#
# uses global CLDB_RUNNING, CD_DRILLBITS_ROLE
#############################################################################
function configureDrillBitsJMX() {
    local rc1
    local DRILL_VER
    local DRILL_ENV
    local jmxAlreadyEnabled=0
    local jmxSecured=0
    local securityChanged=0

    # Enable JMX for Drill server only if they are installed
    # DRILL_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=6090"
    # DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS $DRILL_JMX_OPTS"
    if [ ${CD_DRILLBITS_ROLE} -eq 1 ]; then
        # only change the script once
        DRILL_VER=$(cat "$MAPR_HOME/drill/drillversion")
        DRILL_MAJ_VER=$(echo $DRILL_VER | cut -d . -f 1)
        DRILL_MIN_VER=$(echo $DRILL_VER | cut -d . -f 2)
        if [ -z "$DRILL_MAJ_VER" -o -z "$DRILL_MIN_VER" ]; then
            logWarn "Failed to enable jmx for Drill - couldn't determine version"
            return
        fi
        if [ $DRILL_MAJ_VER -le 1 -a $DRILL_MIN_VER -le 6 ]; then
            DRILL_ENV="${MAPR_HOME}/drill/drill-${DRILL_VER}/conf/drill-env.sh"
            DRILL_TAG="SERVER_GC_OPTS="
            DRILL_AWK_SCRIPT=configureDrillJmx.awk
        else
            DRILL_ENV="${MAPR_HOME}/drill/drill-${DRILL_VER}/conf/distrib-env.sh"
            DRILL_TAG="HADOOP_HOME="
            DRILL_AWK_SCRIPT=configureDrill18Jmx.awk
        fi
        if grep "^#Enable JMX for MaprMonitoring" ${DRILL_ENV} > /dev/null 2>&1; then
            jmxAlreadyEnabled=1
        fi
        if grep "^DRILL_JMX_OPTS=" ${DRILL_ENV} | grep "jmxremote.authenticate=true"> /dev/null 2>&1; then
            jmxSecured=1
        fi
        if [ \( ${secureCluster} -eq 0 -a $jmxSecured -eq 1 \) -o \( ${secureCluster} -eq 1 -a $jmxSecured -eq 0 \) ]; then
            securityChanged=1
        fi

        # only change the script once or when security config changed
        if [ $jmxAlreadyEnabled -eq 0 -o \( $jmxAlreadyEnabled -eq 1 -a $securityChanged -eq 1 \) ]; then
            # save backup copy fist time only
            if [ $jmxAlreadyEnabled -eq 0 ]; then
                cp -p ${DRILL_ENV} ${DRILL_ENV}.prejmx
    
                DRILL_SECURE_JMX="false"
                if [ $secureCluster -eq 1 ]; then
                    DRILL_SECURE_JMX="true"
                fi
        
                awk -v jmx_insert_after="$DRILL_TAG" \
                    -f ${AWKLIBPATH}/${DRILL_AWK_SCRIPT} -vmapr_home=${MAPR_HOME} -vdrillport=$DRILLBITS_JMX_PORT -vsecurejmx=$DRILL_SECURE_JMX ${DRILL_ENV} > ${DRILL_ENV}.tmp
                rc1=$?
                if [ $rc1 -eq 0 ]; then
                    mv ${DRILL_ENV}.tmp ${DRILL_ENV}
                    chmod a+x ${DRILL_ENV}
                    CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST drill-bits"
                else
                    logWarn "Failed to enable jmx for Drill Server - see ${DRILL_ENV}.tmp"
                fi
            else
                if [ $jmxSecured -eq 1 ]; then
                    cur_auth_val="true"
                    new_auth_val="false"
                else
                    cur_auth_val="false"
                    new_auth_val="true"
                fi
                sed -i -e "s/\.jmxremote\.authenticate=$cur_auth_val/\.jmxremote\.authenticate=$new_auth_val/" ${DRILL_ENV}
                CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST drill-bits"
            fi
        fi
    fi
}

#############################################################################
# Function to configure Oozie JMX
#
# uses global CD_OOZIE_ROLE
#############################################################################
function configureOozieJMX() {
    local oozie_restart=0
    local rc0=0
    local rc1=0

    if [ ${CD_OOZIE_ROLE} -eq 1 ]; then
        OOZIE_VER=$( cat $MAPR_HOME/oozie/oozieversion )
        OOZIE_HOME="$MAPR_HOME/oozie/oozie-$OOZIE_VER"
        OOZIE_ENV="$OOZIE_HOME/conf/oozie-env.sh"
        if ! fgrep org.apache.oozie.service.MetricsInstrumentationService \
            $OOZIE_HOME/conf/oozie-site.xml  > /dev/null 2>&1 ; then

            cp $OOZIE_HOME/conf/oozie-site.xml $OOZIE_HOME/conf/oozie-site.xml.$CD_NOW
            sed -i -e 's/<\/configuration>/    <property>\n        <name>oozie.services.ext<\/name>\n        <value>\n            org.apache.oozie.service.MetricsInstrumentationService\n        <\/value>\n    <\/property>\n\n<\/configuration>/' \
               $OOZIE_HOME/conf/oozie-site.xml
            rc0=$?
            if [ $rc1 -eq 0 ]; then
                oozie_restart=1
            fi
        fi
        if ! grep "^export CATALINA_OPTS" ${OOZIE_ENV} | grep jmxremote.authenticate > /dev/null 2>&1; then
            cp -p ${OOZIE_ENV} ${OOZIE_ENV}.prejmx

            sed -i -e '/export CATALINA_OPTS/a export CATALINA_OPTS=\"\$CATALINA_OPTS '"${OOZIE_JMX_INSERT//\$MAPR_HOME/$MAPR_HOME}" $OOZIE_ENV
            rc1=$?
            if [ $rc1 -eq 0 ]; then
                oozie_restart=1
            fi
         fi
         if [ $rc0 -ne 0 -o $rc1 -ne 0 ]; then
             logWarn "Failed to enable jmx for Oozie Server - see ${OOZIE_ENV}.tmp"
         fi
         if [ $secureCluster -eq 1 ]; then
            secureVal="true"
            oldSecureVal="false"
         else
            secureVal="false"
            oldSecureVal="true"
         fi
         sed -i -e "s/\(jmxremote.authenticate=\)$oldSecureVal/\1$secureVal/" $OOZIE_ENV
         oozie_restart=1
    fi
    if [ $oozie_restart -eq 1 ]; then
        CD_RESTART_SVC_LIST="$CD_RESTART_SVC_LIST oozie"
    fi
}

#############################################################################
# Function to check to see if a service is running
#
# $1 is the node name/ip
# $2 is the service name
#############################################################################
function isMaprServiceRunning() {
   local myHname
   local serviceNameToCheck
   local serviceStatus
   
   myHname=$1
   serviceNameToCheck=$2
 
   if [ $secureCluster -eq 1 ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
       export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
   fi

   # using $NF with awk instead of $5 because memallocated column doesn't always
   # contain data
   serviceStatus=$( maprcli service list -node $myHname | fgrep $serviceNameToCheck | awk '{ print $NF }' )
   if [ "$serviceStatus" == "2" ]; then
       return 0
   else
       return 1
   fi

}

#############################################################################
# Function to restart services after JMX is enabled
#
# uses globals CLDB_RUNNING, CD_RESTART_SVC_LIST
#############################################################################
function restartServices() {
    local MyHname

    if safeToRunMaprCLI; then
        MyHname=$(hostname -f)
        if [ -z "$MyHname" ]; then
            # some aws machine reports an empty string with hostname -f
            MyHname=$(hostname) 
        fi

        # restart services that we changed configuration files for
        # but only if they are currently running
        for svc in $CD_RESTART_SVC_LIST ; do
            if isMaprServiceRunning $MyHname $svc ; then
                maprcli node services -nodes ${MyHname} -name $svc -action restart
            fi
        done
    fi
}

#############################################################################
# Function to wait for cldb to come up
#
# Sets global CLDB_RUNNING
#############################################################################
function waitForCLDB() {
    local cldbretries
    cldbretries=${CLDB_RETRIES}   # give it a minute
    until [ $CLDB_RUNNING -eq 1 -o $cldbretries -lt 0 ]; do
        $MAPR_HOME/bin/maprcli node cldbmaster > /dev/null 2>&1
        [ $? -eq 0 ] && CLDB_RUNNING=1
        [ $CLDB_RUNNING -ne 0 ] &&  sleep $CLDB_RETRY_DLY
        let cldbretries=cldbretries-1
    done
    return $CLDB_RUNNING
}



#############################################################################
# Function to create conf directory for custom collectd.conf
#
#############################################################################
function createCustomConfDirectory()
{
    if  ! [ -d ${COLLECTD_CUSTOM_CONF_DIR} ]; then
        mkdir -p ${COLLECTD_CUSTOM_CONF_DIR} > /dev/null 2>&1
    fi

    chown -R $MAPR_USER:$MAPR_GROUP ${COLLECTD_CUSTOM_CONF_DIR}
}


#############################################################################
# Function to install warden config file in $MAPR_CONF_DIR
#
#############################################################################
function installWardenConfFile()
{
    if  ! [ -d ${MAPR_CONF_DIR}/conf.d ]; then
        mkdir -p ${MAPR_CONF_DIR}/conf.d > /dev/null 2>&1
    fi

    cp ${COLLECTD_HOME}/etc/conf/warden.collectd.conf ${MAPR_CONF_DIR}/conf.d/
    chown $MAPR_USER:$MAPR_GROUP ${MAPR_CONF_DIR}/conf.d/warden.collectd.conf
}

#############################################################################
# Function to clean up old files
#
#############################################################################
function cleanupOldConfFiles
{
    # XXX should to remove a subset of the dated backups
    rm -f ${NEW_CD_CONF_FILE}
}



# main
#
# typically called from master configure.sh with the following arguments
#
# configure.sh  -nodecount ${otNodesCount} -OT "${otNodesList}" -nodePort ${otPort}
#
# we need will use the roles file to know if this node is a RM. If this RM
# is not the active one, we will be getting 0s for the stats.
#
#sets MAPR_USER/MAPR_GROUP/logfile
initCfgEnv
JMX_REMOTE_PASSWORD_FILE="${MAPR_CONF_DIR}/jmxremote.password"

usage="usage: $0 [-nodeCount <cnt>] [-nodePort <port>] [-noStreams] [-EC <commonEcoOpts>]\n\t[--secure] [--customSecure] [--unsecure] [-R] [-OS] [-OT \"ip:port,ip1:port,\"] "
if [ ${#} -gt 1 ]; then
    # we have arguments - run as as standalone - need to get params and
    OPTS=`getopt -a -l EC: -l help -l nodeCount: -l nodePort: -l noStreams -l OS -l OT: -l secure -l R -l unsecure -l customSecure -- "$@"`
    if [ $? != 0 ]; then
        echo -e ${usage}
        return 2 2>/dev/null || exit 2
    fi
    eval set -- "$OPTS"

    for i in "$@" ; do
        case "$i" in
            --EC)
                #Parse Common options
                #Ingore ones we don't care about
                ecOpts=($2)
                shift 2
                restOpts="$@"
                eval set -- "${ecOpts[@]} --"
                for j in "$@" ; do
                    case "$j" in
                        --OT|-OT)
                            nodelist="$2"
                            shift 2;;
                        --R|-R)
                            CD_CONF_ASSUME_RUNNING_CORE=1
                            shift 1
                            ;;
                        --noStreams|-noStreams)
                            useStreams=0;
                            shift 1;;
                        --) shift
                            break;;
                        *)
                            #echo "Ignoring common option $j"
                            shift 1;;
                    esac
                done
                shift 2 
                eval set -- "$restOpts"
                ;;
            --OS)
                useStreams=1;
                shift 1;;
            --OT)
                nodelist="$2";
                shift 2;;
            --R)
                CD_CONF_ASSUME_RUNNING_CORE=1
                shift 1;;
            --nodeCount)
                nodecount="$2";
                shift 2;;
            --nodePort)
                nodeport="$2";
                shift 2;;
            --noStreams)
                useStreams=0;
                shift 1;;
            --customSecure)
                if [ -f "$COLLECTD_HOME/etc/.not_configured_yet" ]; then
                    # collectd added after secure 5.x cluster upgraded to customSecure
                    # 6.0 cluster. Deal with this by assuming a regular --secure path
                    :
                else 
                    # this is a little tricky. It either means a simpel configure.sh -R run
                    # or it means that collectd was part of the 5.x to 6.0 upgrade
                    # At the moment collectd knows of no other security settings besides jmx
                    # and port numbers the jmx uses. Since we have no way of detecting what 
                    # these ports are - we assume for now they don't change.
                    :
                fi
                secureCluster=1;
                shift 1;;
            --secure)
                secureCluster=1;
                shift 1;;
            --unsecure)
                secureCluster=0;
                shift 1;;
            --help)
                echo -e ${usage}
                return 2 2>/dev/null || exit 2
                ;;
            --)
                shift
                break;;
        esac
    done

else
    echo -e "${usage}"
    return 2 2>/dev/null || exit 2
fi

if [ -z "$nodelist" -a $useStreams -eq 0 ]; then
    echo "-OT or -OS is required"
    echo -e "${usage}"
    return 2 2>/dev/null || exit 2
fi
# Make a copy, the script will work on the copy
cp ${CD_CONF_FILE} ${NEW_CD_CONF_FILE}

# These are disabled for now
#configurehostname
#configureinterfaceplugin
#configurediskplugin
#configurezookeeperconfig
adjustOwnership
getRoles
configuremaprstreamsplugin  # this ucomments everything between the MAPR_CONF_TAGs
configureopentsdbplugin  # this ucomments everything between the MAPR_CONF_TAGs
configurejavajmxplugin
#createFastJMXLink
configureHadoopJMX
configureHbaseJMX
configureDrillBitsJMX
configureOozieJMX
if [ $CD_CONF_ASSUME_RUNNING_CORE -eq 1 ]; then
    if safeToRunMaprCLI ; then
        waitForCLDB
        restartServices
    fi
fi

cp -p ${CD_CONF_FILE} ${CD_CONF_FILE}.${CD_NOW}
cp ${NEW_CD_CONF_FILE} ${CD_CONF_FILE}
chmod 640 ${CD_CONF_FILE}
installWardenConfFile
# remove state file
if [ -f "$COLLECTD_HOME/etc/.not_configured_yet" ]; then
    rm -f "$COLLECTD_HOME/etc/.not_configured_yet"
fi
createCustomConfDirectory
cleanupOldConfFiles
true # make sure we have a good return
