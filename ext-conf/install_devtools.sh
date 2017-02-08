#!/bin/bash
set -x 
MEPVER=${MEPVER:-3.0.0}
COREVER=${COREVER:-5.2.1}
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
    yum -y install yum-plugin-downloadonly
    checkerror "Failed to install yum-plugin-download"
    yum -y install --downloadonly --downloaddir=/tmp/cache mapr-librdkafka
    checkerror "Failed to download mapr-librdkafka"
    rpm -i --nodeps /tmp/cache/mapr-librdkafka*
    checkerror "Failed to install mapr-librdkafka"
    yum -y install --downloadonly --downloaddir=/tmp/cache mapr-client
    checkerror "Failed to download mapr-client"
    rpm -i --nodeps /tmp/cache/mapr-client*
    checkerror "Failed to install mapr-client"
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
    apt-get update
    dpkg -l | fgrep mapr
    apt-get -y -m install mapr-librdkafka
    checkerror "Failed to install mapr-librdkafka"
    apt-get -y -m install mapr-client
    checkerror "Failed to install mapr-client"
fi

ls -l /opt/mapr/lib
