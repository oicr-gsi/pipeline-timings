#!/bin/bash 


module load mongodb-database-tools/100.5.2 
module load jq

# Create output directory
mkdir Extracted_Metrics

while IFS= read -r line; do 
   
  query_str='{"workflow_run_id": "'"$line"'"}' 
  echo $query_str
  time mongoexport --host workflow-metrics-db.gsi.oicr.on.ca --port 27017 --username workflow_metrics_ro --config /.mounts/labs/gsi/secrets/workflow-metrics-db.gsi_workflow_metrics_ro --db workflow_metrics --collection production_cromwell_workflow_metrics --jsonArray --query "$query_str"| jq "." > "Extracted_Metrics/$line.json"


done < workflow_ids.txt
