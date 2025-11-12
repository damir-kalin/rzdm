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
  echo "Cancelling ${CLASS} job"
  curl -X POST http://${JM_ADDRESS}:${JM_PORT}/jobs/cancel -d "name=${CLASS}"
done