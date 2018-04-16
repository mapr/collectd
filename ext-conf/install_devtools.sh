#!/bin/bash
set -x 
MEPVER=${MEPVER:-5.0.0}
COREVER=${COREVER:-6.0.1}
MEPREPO=http://artifactory.devops.lab/artifactory/prestage/releases-dev/MEP/MEP-${MEPVER}
COREREPO=http://artifactory.devops.lab/artifactory/prestage/releases-dev/v${COREVER}

function checkerror() {
    if [ $? -ne 0 ]; then
        echo "$1"
        exit 1
    fi
}


if [ -f /etc/redhat-release ]; then
    cat > /etc/yum.repos.d/mapr_mep.repo <<EOF
[MapR_Ecosystem]
name = MapR Ecosystem Components
baseurl = $MEPREPO/redhat
gpgcheck = 0
enabled = 1
protected = 1
EOF
    checkerror "Failed to install MEP repo"
    cat /etc/yum.repos.d/mapr_mep.repo
    cat > /etc/yum.repos.d/mapr_core.repo <<EOFC
[MapR_Core]
name = MapR Core Components
baseurl = $COREREPO/redhat
gpgcheck = 0
enabled = 1
protected = 1
EOFC

    checkerror "Failed to install Core repo"
    cat /etc/yum.repos.d/mapr_core.repo
    yum clean all
    rpm -qa | fgrep mapr
    yum -y remove mapr-*
    yum -y install yum-plugin-downloadonly
    checkerror "Failed to install yum-plugin-download"
    yum -y install --downloadonly --downloaddir=/tmp/cache mapr-core
    checkerror "Failed to download mapr-core"
    rpm -i --nodeps /tmp/cache/mapr-core*
    checkerror "Failed to install mapr-core"
    yum -y install --downloadonly --downloaddir=/tmp/cache mapr-hadoop-core
    checkerror "Failed to download mapr-hadoop-core"
    rpm -i --nodeps /tmp/cache/mapr-hadoop-core*
    checkerror "Failed to install mapr-hadoop-core"
    yum -y install --downloadonly --downloaddir=/tmp/cache mapr-librdkafka
    checkerror "Failed to download mapr-librdkafka"
    rpm -i --nodeps /tmp/cache/mapr-librdkafka*
    checkerror "Failed to install mapr-librdkafka"

    yum -y install --downloadonly --downloaddir=/tmp/cache protobuf-devel
    checkerror "Failed to download protobuf-devel"
    rpm -i --nodeps /tmp/cache/protobuf*
    checkerror "Failed to install protobuf (c++ compiler, headers, library)"
    rm /tmp/cache/*

    yum -y install --downloadonly --downloaddir=/tmp/cache protobuf-c-devel
    checkerror "Failed to download protobuf-c-devel"
    rpm -i --nodeps /tmp/cache/protobuf-c-*
    checkerror "Failed to install protobuf-c (c compiler, headers, library)"
    rm /tmp/cache/*


else
    cat > /etc/apt/sources.list.d/mapr_mep.list <<EOR
deb $MEPREPO/ubuntu binary trusty
EOR
    checkerror "Failed to install MEP repo"
    cat /etc/apt/sources.list.d/mapr_mep.list
    cat > /etc/apt/sources.list.d/mapr_core.list <<EORC
deb $COREREPO/ubuntu binary trusty
EORC
    cat /etc/apt/sources.list.d/mapr_core.list
    apt-key adv --fetch-keys http://package.mapr.com/releases/pub/maprgpg.key
    apt-get update
    apt-get -y purge mapr-*
    dpkg -l | fgrep mapr
    apt-get -y -m install mapr-core
    checkerror "Failed to install mapr-core"
    apt-get -y -m install mapr-librdkafka
    checkerror "Failed to install mapr-librdkafka"

    apt-get -y install protobuf-compiler
    checkerror "Failed to install protobuf-compiler"
    apt-get -y install protobuf-c-compiler
    checkerror "Failed to install protobuf-c-compiler"

    apt-get -y install libprotobuf-dev
    checkerror "Failed to install libprotobuf-dev"
    apt-get -y install libprotobuf-c0-dev
    checkerror "Failed to install libprotobuf-c0-dev"

fi

ls -l /opt/mapr/lib
