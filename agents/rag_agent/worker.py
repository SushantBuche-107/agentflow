
from agents.common.base_agent import BaseAgent
from agents.common.logger import get_logger
from .vector_store import VectorStore
logger = get_logger('RAGAgent')

class RAGAgent(BaseAgent):
    def __init__(self, context):
        super().__init__(context)
        self.store = VectorStore()

    def run(self, payload):
        query = payload.get('query')
        logger.info(f'Searching for query: {query}')
        results = self.store.search(query)
        return {'status': 'SUCCESS', 'output': {'results': results}, 'metrics': {'latency_ms': 150}}
