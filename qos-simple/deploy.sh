#!/bin/sh -e

onos-app 192.168.56.5 install target/qos-simple-1.0-SNAPSHOT.oar

onos-app 192.168.56.5 activate org.qos.simple
