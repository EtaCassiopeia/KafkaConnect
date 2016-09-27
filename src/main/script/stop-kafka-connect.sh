#!/usr/bin/env bash

PID_FILE="/tmp/HDFS-Connector.pid"
start-stop-daemon --stop --quiet --retry=TERM/30/KILL/5 --pidfile $PID_FILE
rm -rf $PID_FILE