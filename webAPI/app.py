from fastapi import FastAPI
from dagster import execute_pipeline
from dagster.core.execution.api import create_execution_plan, execute_plan
from repo import process_csv_pipeline

app = FastAPI()

@app.get("/run-pipeline")
async def run_pipeline():
    result = execute_pipeline(process_csv_pipeline)

    if result.success:
        return {"status": "success", "run_id": result.run_id}
    else:
        return {"status": "failure", "run_id": result.run_id}