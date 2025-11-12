#!/bin/bash
echo "Copying streaming-data-pipeline-1.0-SNAPSHOT.jar to rzdm/flink-1762357787-jobmanager-559b78b6f6-ftcr2"
kubectl cp target/streaming-data-pipeline-1.0-SNAPSHOT.jar rzdm/flink-1762357787-jobmanager-559b78b6f6-ftcr2:/opt/usrlib/streaming-data-pipeline-1.0-SNAPSHOT.jar
kubectl cp submit-job.sh rzdm/flink-1762357787-jobmanager-559b78b6f6-ftcr2:/opt/usrlib/submit-job.sh
echo "Copying submit-job.sh to rzdm/flink-1762357787-jobmanager-559b78b6f6-ftcr2"
kubectl exec -it rzdm/flink-1762357787-jobmanager-559b78b6f6-ftcr2 -- bash -c "chmod +x /opt/usrlib/submit-job.sh"
echo "Running submit-job.sh"
kubectl exec -it rzdm/flink-1762357787-jobmanager-559b78b6f6-ftcr2 -- bash -c "/opt/usrlib/submit-job.sh"