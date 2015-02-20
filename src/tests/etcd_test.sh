#!/usr/bin/env bash

# Find MESOS_SOURCE_DIR and MESOS_BUILD_DIR.
env | grep MESOS_SOURCE_DIR >/dev/null

if [ "${?}" -ne 0 ]; then
  echo "Failed to find MESOS_SOURCE_DIR in environment"
  exit 1
fi

env | grep MESOS_BUILD_DIR >/dev/null

if [ "${?}" -ne 0 ]; then
  echo "Failed to find MESOS_BUILD_DIR in environment"
  exit 1
fi

source ${MESOS_SOURCE_DIR}/support/atexit.sh

# Create a work directory in our local testing directory so that
# everything gets cleaned up after the tests complete.
WORK_DIR=`pwd`/work_dir

OS=`uname -s`

ETCD=etcd
ETCDCTL=etcdctl

# Make sure etcd exists, othewise try and download.
${ETCD} --help >/dev/null 2>&1

if [ "${?}" -ne 0 ]; then
  echo "Need to download etcd"
  if [ "${OS}" == 'Linux' ]; then
    curl -L https://github.com/coreos/etcd/releases/download/v2.0.0/etcd-v2.0.0-linux-amd64.tar.gz -o etcd-v2.0.0-linux-amd64.tar.gz
    tar xzvf etcd-v2.0.0-linux-amd64.tar.gz
    ETCD=./etcd-v2.0.0-linux-amd64/etcd
    ETCDCTL=./etcd-v2.0.0-linux-amd64/etcdctl
  elif [ "${OS}" == 'Darwin' ]; then
    curl -L https://github.com/coreos/etcd/releases/download/v2.0.0/etcd-v2.0.0-darwin-amd64.zip -o etcd-v2.0.0-darwin-amd64.zip
    unzip etcd-v2.0.0-darwin-amd64.zip
    ETCD=./etcd-v2.0.0-darwin-amd64/etcd
    ETCDCTL=./etcd-v2.0.0-darwin-amd64/etcdctl
  else
    echo "Unsupported platfrom '${OS}', exiting"
    exit -1
  fi
fi

# Start etcd and make sure it exits when we do.
${ETCD} -data-dir=${WORK_DIR}/etcd &

atexit "kill ${!}"

echo "Started etcd"

# Now give etcd a chance to come up properly so we can use etcdctl
# without it failing because etcd isn't accepting connections yet.
sleep 10

common_flags=(
  # List of nodes for replicated log. Tests using the default port (5050)
  # with the first entry.
  --ip=127.0.0.1
  --masters=127.0.0.1,127.0.0.1:6060
  --etcd=etcd://127.0.0.1/v2/keys/mesos
)

# Start watching etcd so that we can check our expectations.
#${ETCDCTL} watch /mesos >/dev/null 2>&1 &
${ETCDCTL} watch /mesos &

WATCH=${!}

atexit "kill ${WATCH}"

# First Mesos master, explicit quorum size. Should become leader in
# election.
${MESOS_BUILD_DIR}/src/mesos-master "${common_flags[@]}" \
  --quorum=2 \
  --work_dir="${WORK_DIR}/master1" &

MASTER1=${!}

atexit "kill ${MASTER1}"

# Wait for the watch to terminate since the first master should become
# elected on its own.
# TODO(benh): This will BLOCK forever if we have a bug!
wait ${WATCH}

if [ "${?}" -ne 0 ]; then
  echo "Failed to wait for the first master to become elected"
  exit -1
fi

sleep 3

# Check that the master has become elected.
curl http://127.0.0.1:5050/stats.json | grep '"elected":1'

if [ "${?}" -ne 0 ]; then
  echo "Expecting the first master to be elected!"
  exit -1
fi

echo "---------------- FIRST MASTER ELECTED ----------------"

# Save the etcd key so that we can compare it when the masters change.
ETCD_MESOS_KEY=`${ETCDCTL} get /mesos`

# TODO: CHECK THE VALUE IN ETCD_MESOS_KEY.
echo ${ETCD_MESOS_KEY}

echo "---------------- FIRST MASTER ELECTED ----------------"





# Again watch etcd so that we can check our expectations.
${ETCDCTL} watch /mesos &

WATCH=${!}

# Now start a second Mesos master but let it determine the quorum size
# implicitly based on --masters.
${MESOS_BUILD_DIR}/src/mesos-master "${common_flags[@]}" \
  --quorum=2 \
  --work_dir="${WORK_DIR}/master2" \
  --port=6060 &

MASTER2=${!}

atexit "kill ${MASTER2}"

# And start a slave.
${MESOS_BUILD_DIR}/src/mesos-slave \
  --master=etcd://127.0.0.1/v2/keys/mesos \
  --resources="cpus:2;mem:10240" \
  --work_dir="${WORK_DIR}/slave" \
  --port=5052 &

atexit "kill ${!}"

sleep 5 # Wait for the master to register and the slaves to recover.

# Now run the test framework using etcd to find the master.
${MESOS_BUILD_DIR}/src/test-framework --master=etcd://127.0.0.1/v2/keys/mesos

if [ "${?}" -ne 0 ]; then
  echo "Expecting the test framework to exit successfully!"
  exit -1
fi

# Now shutdown the first master so that we can check that the second
# master becomes the leader.
kill ${MASTER1}

# Wait for the watch to terminate since the second master should now
# be becoming elected.
# TODO(benh): This will BLOCK forever if we have a bug!
if [ `wait ${WATCH}` -ne 0 ]; then
  echo "Failed to wait for the second master to become elected"
  exit -1
fi

# Check that the second master has become elected.
curl http://127.0.0.1:6060/stats.json | grep '"elected":1'

# Restart the first master and check that it's not elected.
${MESOS_BUILD_DIR}/src/mesos-master "${common_flags[@]}" \
  --quorum=2 \
  --work_dir="${WORK_DIR}/master1" &

MASTER1=${!}

atexit "kill ${MASTER1}"

# Now re-run the test framework using etcd to find the master.
${MESOS_BUILD_DIR}/src/test-framework --master=etcd://127.0.0.1/v2/keys/mesos

if [ "${?}" -ne 0 ]; then
  echo "Expecting the test framework to exit successfully!"
  exit -1
fi

# TODO(cmaloney): Test restarting etcd.

# The atexit handlers will clean up the remaining masters/slaves/etcd.