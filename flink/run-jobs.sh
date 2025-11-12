#!/bin/bash
NAMESPACE="rzdm"
JOB_MANAGER_NAME="jobmanager"
JOB_MANAGER_POD=$(kubectl get pods -n $NAMESPACE | grep $JOB_MANAGER_NAME | awk '{print $1}')

echo "Copying streaming-data-pipeline-1.0-SNAPSHOT.jar to ${JOB_MANAGER_POD}"
kubectl cp target/streaming-data-pipeline-1.0-SNAPSHOT.jar ${NAMESPACE}/${JOB_MANAGER_POD}:/opt/usrlib/streaming-data-pipeline-1.0-SNAPSHOT.jar
kubectl cp submit-job.sh ${NAMESPACE}${JOB_MANAGER_POD}:/opt/usrlib/submit-job.sh
echo "Copying submit-job.sh to ${JOB_MANAGER_POD}"
kubectl exec -it -n ${NAMESPACE} ${JOB_MANAGER_POD} -- bash -c "chmod +x /opt/usrlib/submit-job.sh"
echo "Running submit-job.sh"
kubectl exec -it -n ${NAMESPACE} ${JOB_MANAGER_POD} -- bash -c "/opt/usrlib/submit-job.sh"