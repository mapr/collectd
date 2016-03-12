#!/bin/bash
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved

#############################################################################
#
# Configure script for collectd
# 
# configures the opentsdb and jmx facilities
# enables/disables the opentsdb and jmx facilities
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

COLLECTD_HOME=${COLLECTD_HOME:-__INSTALL__}
CD_CONF_FILE=${CD_CONF_FILE:-${COLLECTD_HOME}/etc/collectd.conf}
NEW_CD_CONF_FILE=${NEW_CD_CONF_FILE:-${COLLECTD_HOME}/etc/collectd.conf.progress}
CD_CONF_FILE_SAVE_AGE="30"
AWKLIBPATH=${AWKLIBPATH:-$COLLECTD_HOME/lib/awk}
NOW=`date "+%Y%m%d_%H%M%S"`
HADOOP_VER="hadoop-2.7.0"
YARN_BIN="/opt/mapr/hadoop/${HADOOP_VER}/bin/yarn" 
RM_JMX_PORT=8025
NM_JMX_PORT=8027
JMX_INSERT='#Enable JMX\nJMX_OPTS=\"-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port\"'
YARN_JMX_RM_OPT_STR='$JMX_OPTS='${RM_JMX_PORT}
YARN_JMX_NM_OPT_STR='$JMX_OPTS='${NM_JMX_PORT}
MAPR_HOME=${MAPR_HOME:-/opt/mapr}
MAPR_CONF_DIR="${MAPR_HOME}/conf/conf.d"
CD_NM_ROLE=0
CD_CLDB_ROLE=0
CD_RM_ROLE=0
CLDB_RUNNING=0

#############################################################################
# Function to uncomment a section
# $1 is the sectionTag prefix we will use to determine section to uncomment
#############################################################################
function enableSection()
{
   # $1 is the sectionTag prefix we will use to determine section to uncomment  
   cat ${NEW_CD_CONF_FILE} | awk -f ${AWKLIBPATH}/uncommentSection.awk -v tag="$1" > ${NEW_CD_CONF_FILE}.t
   if [[ $? -eq 0 ]] ; then
      mv ${NEW_CD_CONF_FILE}.t ${NEW_CD_CONF_FILE}
   fi
}

#############################################################################
# Function to comment out a section
# $1 is the sectionTag prefix we will use to determine section to comment out
#############################################################################
function disableSection()
{
   # $1 is the sectionTag prefix we will use to determine section to comment out
   cat ${CD_CONF_FILE} | awk -f ${AWKLIBPATH}/commentOutSection.awk -v tag="$1" > ${CD_CONF_FILE}.progress
   if [[ $? -eq 0 ]] ; then
      mv ${CD_CONF_FILE} ${CD_CONF_FILE}.${NOW}
      mv ${CD_CONF_FILE}.progress ${CD_CONF_FILE}
   fi
}

#############################################################################
# Function to remove a section
#
# $1 is the sectionTag prefix we will use to determine section to remove
#############################################################################
function removeSection()
{
   cat ${CD_CONF_FILE} | awk -f ${AWKLIBPATH}/removeSection.awk -v tag="$1" > ${CD_CONF_FILE}.progress
   if [[ $? -eq 0 ]] ; then
      mv ${CD_CONF_FILE} ${CD_CONF_FILE}.${NOW}
      mv ${CD_CONF_FILE}.progress ${CD_CONF_FILE}
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
   cat ${CD_CONF_FILE} | awk -f ${AWKLIBPATH}/replaceSection.awk -v tag="$1" -v newSectionContentFile="$2" > ${CD_CONF_FILE}.progress
   if [[ $? -eq 0 ]] ; then
      mv ${CD_CONF_FILE} ${CD_CONF_FILE}.${NOW}
      mv ${CD_CONF_FILE}.progress ${CD_CONF_FILE}
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
   hostn=`hostname -fqdn`
   sed -i -e 's/#Hostname.*$/Hostname' ${hostn}/ ${NEW_CD_CONF_FILE}
}

#############################################################################
# Function to figure out what roles this node has
#
# sets globals
# CD_NM_ROLE
# CD_CLDB_ROLE
# CD_RM_ROLE
#############################################################################
function getRoles() {
   [ -f ${MAPR_HOME}/roles/resourcemanager ] && CD_RM_ROLE=1
   [ -f ${MAPR_HOME}/roles/nodemanager ] && CD_NM_ROLE=1
   [ -f ${MAPR_HOME}/roles/cldb ] && CD_CLDB_ROLE=1
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
   loopifs=`ls /dev/loop[0-9]*`
   awk '/<Plugin interface>/ { next;next; print \tInterface \"${loopifs}\"; print \tIngoreSelection true' ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.t
   mv ${NEW_CD_CONF_FILE}.t ${NEW_CD_CONF_FILE}
}
 

#############################################################################
# Function to configure Interface Plugin
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
   zoohost=${zkNodesList%%,*}
   zoohost=${zoohost%%:*}
   awk -v hostname=$zoohost -v port=$zkClientPort -v plugin=zookeeper -f ${AWKLIBPATH}/condfigurePlugin.awk ${NEW_CD_CONF_FILE} > ${NEW_CONFIG_FILE}.t
   mv ${NEW_CD_CONF_FILE}.t ${NEW_CD_CONF_FILE}

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
# Function to configure opentsdb plugin
#
# uses nodeslist and nodeport arguments
#############################################################################
function configureopentsdbplugin()
{
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
   tsdbhost=${nodelist%%,*}
   tsdbhost=${tsdbhost%%:*}
   enableSection MAPR_CONF_OT_TAG
   awk -v hostname=$tsdbhost -v port=$nodeport -v plugin=write_tsdb -f ${AWKLIBPATH}/configurePlugin.awk ${NEW_CD_CONF_FILE} > ${NEW_CD_CONF_FILE}.t
   mv ${NEW_CD_CONF_FILE}.t ${NEW_CD_CONF_FILE}

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

  # XXX need more TAGS
  if [ ${CD_RM_ROLE} -eq 1  -o ${CD_NM_ROLE} -eq 1  -o ${CD_CLDB_ROLE} -eq 1 ] ; then
    enableSection MAPR_CONF_JMX_TAG
    sed -i 's@${fastjmx_prefix}@'$COLLECTD_HOME'@g' ${NEW_CD_CONF_FILE}
    configureConnections
  fi
}

#############################################################################
# Function to configure connections
#
# uses global CLDB_ROLE, CD_RM_ROLE, CD_NM_ROLE
#############################################################################
function configureConnections() {
  host_name=`hostname`
  if [ ${CD_CLDB_ROLE} -eq 1 ]; then
      enableSection MAPR_CONN_CONF_CLDB_TAG
  fi
  if [ ${CD_NM_ROLE} -eq 1 ]; then
      enableSection MAPR_CONN_CONF_NM_TAG
  fi
  if [ ${CD_RM_ROLE} -eq 1 ]; then
      enableSection MAPR_CONN_CONF_RM_TAG
  fi
  # XXX Still need to make this stateless
  sed -i -e "s/RESOURCEMGR_IP/${host_name}/g;s/NODEMGR_IP/${host_name}/g;s/CLDB_IP/${host_name}/g" ${NEW_CD_CONF_FILE}
}


#############################################################################
# Function to create link for fast JMX jar
#
# uses global MAPR_HOME
#############################################################################
function createFastJMXLink() {

  local jmx_jar
  jmx_jar=$(find ${MAPR_HOME}/collectd-fast-jmx -name 'fast*SNAPSHOT.jar')
  if [ -n "${jmx_jar}" ] ; then
      ln -s ${jmx_jar} ${COLLECTD_HOME}/lib/fast-jmx-1.1-SNAPSHOT.jar
  fi
}
 

#############################################################################
# Function to configure JMX
#
# uses global CLDB_RUNNING, CD_RM_ROLE, CD_NM_ROLE
#############################################################################
function configureHadoopJMX() {
  # Enable JMX for RM and NM only if they are installed 
  if [ ${CD_RM_ROLE} -eq 1 -o ${CD_NM_ROLE} -eq 1 ] ; then
    cp -p ${YARN_BIN} ${YARN_BIN}.prejmx

    awk -v jmx_ins_after='JAVA_HEAP_MAX' -v jmx_insert="$JMX_INSERT" -v jmx_opts_pattern='"\\$COMMAND" = "resourcemanager"' -v yarn_opts="$YARN_JMX_RM_OPT_STR" -f ${AWKLIBPATH}/configureYarnJmx.awk ${YARN_BIN}.prejmx > ${YARN_BIN}

    cp -p ${YARN_BIN} ${YARN_BIN}.prejmx
    awk  -v jmx_opts_pattern='"\\$COMMAND" = "nodemanager"' -v yarn_opts="$YARN_JMX_NM_OPT_STR" -f ${AWKLIBPATH}/configureYarnJmx.awk ${YARN_BIN}.prejmx > ${YARN_BIN}
  fi
}

#############################################################################
# Function to restart nodemanager and resourcemananger services
#
# uses global CLDB_RUNNING, CD_RM_ROLE, CD_NM_ROLE
#############################################################################
function restartNM_RM_service() {
  if [ $CLDB_RUNNING -eq 1 ]; then
    # Enable JMX for RM and NM only if they are installed 
    if [ ${CD_RM_ROLE} -eq 1 -o ${CD_NM_ROLE} -eq 1 ] ; then
      MyNM_ip=`hostname -i`
      timeout -s HUP 30s $MAPR_HOME/bin/maprcli node cldbmaster -noheader 2> /dev/null
      if [ $? -eq 0 ] ; then
        if [ ${CD_RM_ROLE} -eq 1 ]; then
            maprcli node services -nodes ${MyNM_ip} -name resourcemanager -action restart
        fi
        if [ ${CD_NM_ROLE} -eq 1 ]; then
            maprcli node services -nodes ${MyNM_ip} -name nodemanager -action restart
        fi
      fi
    fi
  fi
}

#############################################################################
# Function to wait for cldb to come up
#
#############################################################################
function waitForCLDB() {
    cldbretries=12   # give it a minute
    until [ $CLDB_RUNNING -eq 1 -o $cldbretries -lt 0 ] ; do
        $MAPR_HOME/bin/maprcli node cldbmaster > /dev/null 2>&1 
        [ $? -eq 0 ] && CLDB_RUNNING=1
        [ $CLDB_RUNNING -ne 0 ] &&  sleep 5
        let cldbretries=cldbretries-1
    done
    return $CLDB_RUNNING
}

#############################################################################
# Function to configure clusterID
#
# uses global CLDB_RUNNING
#############################################################################
function configureClusterId() {
    if [ $CLDB_RUNNING -eq 1 ] ; then
        CLUSTER_ID=`cat /opt/mapr/conf/clusterid`
        sed -i 's/\"clusterid=.*/\"clusterid='$CLUSTER_ID'\"/g' ${NEW_CD_CONF_FILE}
    fi
}

#############################################################################
# Function to install warden config file in $MAPR_CONF_DIR
#
#############################################################################
function installWardenConfFile()
{
   if  ! [ -d ${MAPR_CONF_DIR} ]; then
       mkdir -p ${MAPR_CONF_DIR} > /dev/null 2>&1
   fi

   cp ${COLLECTD_HOME}/etc/conf/warden.collectd.conf ${MAPR_CONF_DIR}
}

#############################################################################
# Function to clean up old files
# 
#############################################################################
function cleanupoldconffiles
{
  :
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

usage="usage: $0 -nodeCount <cnt> -OT \"ip:port,ip1:port,\" -nodePort <port> "
if [ ${#} -gt 1 ] ; then
   # we have arguments - run as as standalone - need to get params and
   # XXX why do we need the -o to make this work?
   OPTS=`getopt -a -o h -l nodeCount: -l nodePort: -l OT: -- "$@"`
   if [ $? != 0 ] ; then
      echo ${usage}
      return 2 2>/dev/null || exit 2
   fi
   eval set -- "$OPTS"

   for i ; do
      case "$i" in
         --nodeCount) 
              nodecount="$2";
              shift 2;;
         --OT)
              nodelist="$2"; 
              shift 2;;
         --nodePort)
              nodeport="$2"; 
              shift 2;;
         -h)
              echo ${usage}
              return 2 2>/dev/null || exit 2
              ;;
         --)
              shift;;
      esac
   done

else
   echo "${usage}"
   return 2 2>/dev/null || exit 2
fi

cp ${CD_CONF_FILE} ${NEW_CD_CONF_FILE}

# These are disabled for now
#configurehostname
#configureinterfaceplugin
#configurediskplugin
#configurezookeeperconfig
getRoles
configureopentsdbplugin  # this ucomments everything between the MAPR_CONF_TAGs
configurejavajmxplugin
createFastJMXLink
configureHadoopJMX
waitForCLDB
restartNM_RM_service
configureClusterId

cp -p ${CD_CONF_FILE} ${CD_CONF_FILE}.${NOW}
cp ${NEW_CD_CONF_FILE} ${CD_CONF_FILE}
installWardenConfFile

true # make sure we have a good return
