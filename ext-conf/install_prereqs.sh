#!/bin/bash

COMMON_PKGS="git vim gcc make flex bison automake autoconf libtool"
DEB_PKGS="pkg-config libltdl-dev librrd2-dev libsensors4-dev libsnmp-dev libyajl-dev \
          rrdtool liblvm2-dev libatasmart-dev librrd-dev libmysqlclient-dev libldap-dev \
          libcurl4-openssl-dev python-dev"
RH_PKGS="libtool-ltdl-devel libatasmart-devel lm_sensors-devel lvm2-devel \
         rrdtool-devel perl-devel openldap-devel libgcrypt-devel libcap-devel yajl-devel \
         curl-devel libxml2-devel mysql-devel java-1.8.0-openjdk-devel perl-ExtUtils-Embed \
         perl-ExtUtils-MakeMaker"
if uname -a | fgrep Ubuntu > /dev/null 2>&1 ; then
    apt-get install $COMMON_PKGS $DEB_PKGS
elif [ -f /etc/redhat-release ] ; then
    yum install $COMMON_PKGS $RH_PKGS
fi
