
#!/usr/bin/env bash

i=0
timeout=30

while :
do
  	status=$(curl http://${DOCKER_IP}:8080/admin/v2/clusters 2>/dev/null)
    if [ "${status}" == '["standalone"]' ]; then
      printf "\nPulsar ready\n"
      break;
    fi;

    i=$((i + 1))
    if [ ${i} == ${timeout} ]; then
      echo " Timeout waiting for Pulsar to be ready"
      exit -1;
    fi;

    printf .
    sleep 1
done
