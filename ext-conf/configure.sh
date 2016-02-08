!/bin/bash
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
# __INSTALL__ gets expanded to /opt/mapr/collectd/collectd-5.5 during pakcaging
# set COLLECTD_HOME explicitly if running this in a source built env.
#############################################################################

COLLECTD_HOME=${COLLECTD_HOME:-__INSTALL__}
CONF_FILE=${CONF_FILE:-${COLLECTD_HOME}/etc/collectd.conf}
CONF_FILE_SAVE_AGE="30"
AWKLIBPATH=${AWKLIBPATH:-$COLLECTD_HOME/lib/awk}
NOW=`date "+%Y%m%d_%H%M%S"`
exit 0

#############################################################################
# Function to uncomment a section
#############################################################################
function enableSection
{
   # $1 is the sectionTag prefix we will use to determine section to uncomment  
   cat ${CONF_FILE} | awk -f ${AWKLIBPATH}/uncommentSection.awk -v tag="$1" > ${CONF_FILE}.new
   if [[ $? -eq 0 ]] ; then
      mv ${CONF_FILE} ${CONF_FILE}.${NOW}
      mv ${CONF_FILE}.new ${CONF_FILE}
   fi
}

#############################################################################
# Function to comment out a section
#############################################################################
function disableSection
{
   # $1 is the sectionTag prefix we will use to determine section to comment out
   cat ${CONF_FILE} | awk -f ${AWKLIBPATH}/commentOutSection.awk -v tag="$1" > ${CONF_FILE}.new
   if [[ $? -eq 0 ]] ; then
      mv ${CONF_FILE} ${CONF_FILE}.${NOW}
      mv ${CONF_FILE}.new ${CONF_FILE}
   fi
}

#############################################################################
# Function to remove a section
#############################################################################
function removeSection
{
   # $1 is the sectionTag prefix we will use to determine section to remove
   cat ${CONF_FILE} | awk -f ${AWKLIBPATH}/removeSection.awk -v tag="$1" > ${CONF_FILE}.new
   if [[ $? -eq 0 ]] ; then
      mv ${CONF_FILE} ${CONF_FILE}.${NOW}
      mv ${CONF_FILE}.new ${CONF_FILE}
   fi
}
#############################################################################
# Function to fill a section
#############################################################################
function fillSection
{
   # $1 is the sectionTag prefix we will use to determine section to replace
   # $2 is the file containing the new content of the section
   removeSection $1
   cat ${CONF_FILE} | awk -f ${AWKLIBPATH}/replaceSection.awk -v tag="$1" -v newSectionContentFile="$2" > ${CONF_FILE}.new
   if [[ $? -eq 0 ]] ; then
      mv ${CONF_FILE} ${CONF_FILE}.${NOW}
      mv ${CONF_FILE}.new ${CONF_FILE}
   fi
}

function configureOpenTSDBPlugin
{
}

function configureJavaJMXPlugin
{
}


function configureDiskPlugin
{
}

function cleanupOldConfFiles
{
}
