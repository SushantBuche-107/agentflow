
from fastapi import FastAPI, HTTPException
from compiler.workflow_compiler import compile_workflow
from scheduler.scheduler import execute_workflow_async

app = FastAPI(title='AgentFlow Orchestrator')

@app.post('/api/v1/flows/run')
async def run_workflow(payload: dict):
    workflow_yaml = payload.get('workflow_yaml')
    if not workflow_yaml:
        raise HTTPException(status_code=400, detail='workflow_yaml path required')
    workflow = compile_workflow(workflow_yaml)
    result = await execute_workflow_async(workflow, payload.get('input', {}))
    return result
