#!/bin/bash

JM_PORT="8081"
JM_ADDRESS="flink-1762357787-jobmanager"
CLASSES=(
  "com.example.streaming.OrganizationsPipeline"
  "com.example.streaming.ContractorsPipeline"
  "com.example.streaming.ContractorDebtPipeline"
  "com.example.streaming.EmployeePipeline"
  "com.example.streaming.OrderPipeline"
  "com.example.streaming.MovementPipeline"
  "com.example.streaming.FormPipeline"
  "com.example.streaming.TestPipeline"
)

for CLASS in "${CLASSES[@]}"; do

flink run \
  -Drest.address=${JM_ADDRESS} \
  -Drest.port=${JM_PORT} \
  -m "${JM_ADDRESS}:${JM_PORT}" \
  -d \
  -c ${CLASS} \
  -p 1 \
  /opt/usrlib/streaming-data-pipeline-1.0-SNAPSHOT.jar

if [ $? -eq 0 ]; then
  echo "✅ ${CLASS} job submitted successfully"
else
  echo "❌ Failed to submit ${CLASS} job"
fi

done

echo ""
echo "All jobs submitted. Check Flink Web UI at http://${JM_ADDRESS}:${JM_PORT} for job status"