#!/usr/bin/env bash

BIN=`which $0`
BIN=`dirname ${BIN}`
HOME=`cd "$BIN"/..; pwd`

DEFAULT_LIBEXEC_DIR="$HADOOP_HOME"/libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $HADOOP_LIBEXEC_DIR/hadoop-config.sh

PID_FILE="/tmp/HDFS-Connector.pid"
HEAP="-Xmx1g"

CONF_FILE="${HOME}/config/conf.properties"
WORKER_CONF_FILE="${HOME}/config/worker.properties"

JAR_FILE="${HOME}/`ls ${HOME} | grep kafka-connect`"
MAIN_CLASS="org.github.etacassiopeia.kafka.connect.yarn.KafkaConnectDriver"

java ${HEAP} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${HOME}/file.hprof -XX:OnOutOfMemoryError="kill -9 %p" \
     -cp "${JAR_FILE}:${CLASSPATH}" ${MAIN_CLASS} ${CONF_FILE} ${WORKER_CONF_FILE} > /dev/null &

echo $! > $PID_FILE