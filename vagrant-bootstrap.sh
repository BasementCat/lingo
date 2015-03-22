#!/usr/bin/env bash

# MongoDB repo
apt-key adv --keyserver keyserver.ubuntu.com --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/debian-sysvinit dist 10gen' | tee /etc/apt/sources.list.d/mongodb.list

apt-get update
apt-get install -y --force-yes python python-pip python-dev couchdb mongodb-org

# CouchDB config

cat >/etc/couchdb/local.d/10-network.ini <<EOT
[httpd]
port = 5984
bind_address = 0.0.0.0
EOT
chown -R couchdb /etc/couchdb

service couchdb restart

#MongoDB config
cat >/etc/mongod.conf <<EOT
storage:
    dbPath: "/var/lib/mongodb"
    directoryPerDB: true
    journal:
        enabled: true
systemLog:
    destination: file
    path: "/var/log/mongodb/mongod.log"
    logAppend: true
    timeStampFormat: iso8601-utc
processManagement:
    fork: true
net:
    bindIp: 0.0.0.0
    port: 27017
    wireObjectCheck: true
    unixDomainSocket: 
        enabled: false
EOT

service mongod restart

cd /vagrant
sudo python setup.py develop
