import json
import boto3
import os
from datetime import datetime, timezone

glue = boto3.client("glue")

def lambda_handler(event, context):
    job_name = os.environ.get("GLUE_JOB_NAME", "customer_scd2_team2")

    try:
        response = glue.start_job_run(JobName = job_name)
        run_id = response["JobRunId"]
        
        result = {
            "message": "SCD2 batch job triggered!",
            "glue_job_name": job_name,
            "glue_run_id": run_id,
            "trigger_time": datetime.now(timezone.utc).isoformat(),
            "detail": "Glue job has started and will process the latest CDC files from bronze and update the SCD2 dimension parquets in silver."
        }

        print(json.dumps(result, indent=2))
        return result
    except Exception as e:
        error = {
            "message": "Failed to trigger batch SCD2.",
            "glue_job_name": job_name,
            "error": str(e),
            "trigger_time": datetime.now(timezone.utc).isoformat()
        }
        print(json.dumps(error, indent=2))
        return error

