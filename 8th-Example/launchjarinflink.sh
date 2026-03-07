#!/bin/bash
#
# This script is used to upload a JAR file to the Flink JobManager and run it.
# Usage: ./launchjarinflink.sh <path/to/jar_file> <Class_Main_proyecto>
# 
# Returns the Job ID of the submitted job. It should be use to cancel the job if needed.
# In case of using the Job ID to cancel the job, use the following command:
#
#  docker exec -i flink-jobmanager flink cancel <Job_ID>


docker cp $1 flink-jobmanager:/tmp/
FILENAME=$(basename "$1")
JOB_ID=$(
  docker exec -i flink-jobmanager flink run -d -c $2 /tmp/$FILENAME 2>&1 \
  | sed -n 's/.*JobID \([a-f0-9]\{32\}\).*/\1/p' \
  | tail -n1
)
echo $JOB_ID