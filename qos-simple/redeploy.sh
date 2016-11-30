#!/bin/sh -e

onos-app 192.168.56.5 uninstall org.qos.simple

onos-app 192.168.56.5 reinstall target/qos-simple-1.0-SNAPSHOT.oar

onos-app 192.168.56.5 activate org.qos.simple
