#!/bin/bash

MEPVER={-:3.0.0}
MEPREPO=http://artifactory.devops.lab/artifactory/prestage/releases-dev/MEP/MEP-${MEPVER}

function checkerror() {
    if [ $? -ne 0 ]; then
        echo "$1"
        exit 1
    fi
}


if [ -f /etc/redhat-release ]; then
    cat > /etc/yum.repos.d/mapr_mep.list  <<-EOF
[MapR_Ecosystem]
name = MapR Ecosystem Components
baseurl = $MEPREPO/redhat
gpgcheck = 0
enabled = 1
protected = 1
EOF 

    checkerror "Failed to install MEP repo"
    yum clean all
    yum install yum-plugin-downloadonly
    checkerror "Failed to install yum-plugin-download"
    yum --downloadonly --downloaddir=/tmp/cache mapr-librdkafka-*
    checkerror "Failed to download mapr-librdkafka"
    rpm -i --nodeps /tmp/cache/mapr-librdkafka*
    checkerror "Failed to install mapr-librdkafka"
else
    echo > /etc/apt/sources.list.d/mapr_mep.list <<-EOF
deb $MEPREPO/ubuntu binary trusty
EOF
    checkerror "Failed to install MEP repo"
    apt-get update
    apt-get -y --nodeps install mapr-libkafka
    checkerror "Failed to install mapr-librdkafka"
fi
