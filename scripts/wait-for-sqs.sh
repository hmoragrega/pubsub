#!/usr/bin/env bash

i=0
timeout=20

while :
do
  	# Local stack
  	# status=$(curl http://${DOCKER_IP}:4566/health 2>/dev/null)
    # if [ "${status}" == '{"services": {"sqs": "running", "sns": "running"}}' ]; then

    status=$(curl http://${DOCKER_IP}:4100/health 2>/dev/null)
    if [ "${status}" == 'OK' ]; then
      printf "\nSQS & SNS ready"
      break;
    fi;

    i=$((i + 1))
    if [ ${i} == ${timeout} ]; then
      echo " Timeout waiting for SQS to be ready"
      exit -1;
    fi;

    printf .
    sleep 1
done
