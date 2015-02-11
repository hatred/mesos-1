#!/usr/bin/env bash
set -e

#Find MESOS_SOURCE_DIR and MESOS_BUILD_DIR
env | grep MESOS_SOURCE_DIR >/dev/null

test $? != 0 && \
  echo "Failed to find MESOS_SOURCE_DIR in environment" && \
  exit 1

env | grep MESOS_BUILD_DIR >/dev/null

test $? != 0 && \
  echo "Failed to find MESOS_BUILD_DIR in environment" && \
  exit 1

source ${MESOS_SOURCE_DIR}/support/atexit.sh

WORK_DIR=`pwd`/work_dir

echo "Work Dir: $WORK_DIR"

OS=`uname -s`

# If etcd doesn't exist, download it
if hash etcd 2>/dev/null; then
  ETCD_BIN=etcd
else
  # TODO(cmaloney): Test on OS X
  if [[ ! -d etcd-v2.0.0 ]]; then
    if [ "$OS" == 'Linux' ]; then
      curl -L  https://github.com/coreos/etcd/releases/download/v2.0.0/etcd-v2.0.0-linux-amd64.tar.gz -o etcd-v2.0.0-linux-amd64.tar.gz
      tar xzvf etcd-v2.0.0-linux-amd64.tar.gz
      ETCD_BIN="./etcd-v2.0.0-linux-amd64/etcd"
    elif [ "$OS" == 'Darwin' ]; then
      curl -L  https://github.com/coreos/etcd/releases/download/v2.0.0/etcd-v2.0.0-darwin-amd64.zip -o etcd-v2.0.0-darwin-amd64.zip
      unzip etcd-v2.0.0-darwin-amd64.zip
      ETCD_BIN="./etcd-v2.0.0-darwin-amd64/etcd"
    else
      echo "Unsupported platfrom '$OS'."
      echo 'No known way to get etcd. Please write one :)'
      exit -1
    fi
  fi
fi

# Start etcd
"$ETCD_BIN" &
ETCD=$!
atexit "kill $ETCD"

common_flags=(
  # List of nodes for replicated log. Tests using the default port (5050)
  # with the first entry.
  --masters=127.0.0.1,127.0.0.1:5051,127.0.0.1:5052
  --etcd=etcd://127.0.0.1/v2/keys/mesos
  )

# Start 3 mesos masters

# First mesos master, explicit quorum size. Will become leader in election.
"${MESOS_BUILD_DIR}/src/mesos-master" "${common_flags[@]}" \
  --quorum=2 --work_dir="$WORK_DIR/master1" &
MASTER1=$!
atexit "kill $MASTER1"

# Second mesos master, implicit quorum size based on --masters
"${MESOS_BUILD_DIR}/src/mesos-master" "${common_flags[@]}" \
  --work_dir="$WORK_DIR/master2"  --port=5051 &
MASTER2=$!
atexit "kill $MASTER2"

# Third mesos master
"${MESOS_BUILD_DIR}/src/mesos-master" "${common_flags[@]}" \
  --work_dir="$WORK_DIR/master3" --port=5052 &
MASTER3=$!
atexit "kill $MASTER3"

# Set resources for the slave.
export MESOS_RESOURCES="cpus:2;mem:10240"

# Start a slave
"${MESOS_BUILD_DIR}/src/mesos-slave" --master=etcd://127.0.0.1/v2/keys/mesos \
  --work_dir="$WORK_DIR/slave" --port=5053 &
SLAVE=$!
atexit "kill $SLAVE"

# Give things some time to settle
# TODO(cmaloney): Sleep in tests is bad...
sleep 3

echo "Running test framework"

# Test launching a framework against etcd masters, see that it exits cleanly.
"${MESOS_BUILD_DIR}/src/test-framework" --master=etcd://127.0.0.1/v2/keys/mesos

#TODO(cmaloney): Test restarting etcd

echo "Test framework finished"

# atexit handlers will clean up the masters/slaves/etcd