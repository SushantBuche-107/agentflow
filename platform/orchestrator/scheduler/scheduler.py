
import asyncio
from agents.common.logger import get_logger
from msg_bus.kafka_client import send_message
logger = get_logger('scheduler')

async def execute_node(node, context, max_retries=2):
    retries = 0
    while retries <= max_retries:
        try:
            module_path, func_name = node['impl'].rsplit('.', 1)
            module = __import__(module_path, fromlist=[func_name])
            func = getattr(module, func_name)
            result = func(context)
            send_message(node['id'], str(result))
            return result
        except Exception as e:
            retries += 1
            logger.error(f'Node {node["id"]} failed: {e}, retry {retries}')
            await asyncio.sleep(1)
    return {'status': 'FAILED'}

async def execute_workflow_async(workflow, input_data):
    context = {'input': input_data}
    for node in workflow['nodes']:
        result = await execute_node(node, context)
        context[node['id']] = result
    return {'workflow_status': 'COMPLETED', 'context': context}
