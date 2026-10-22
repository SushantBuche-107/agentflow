
from agents.common.base_agent import BaseAgent
from agents.common.logger import get_logger
from .ocr_model import OCRModel
logger = get_logger('InvoiceAgent')

class InvoiceAgent(BaseAgent):
    def __init__(self, context):
        super().__init__(context)
        self.ocr = OCRModel()

    def run(self, payload):
        file_path = payload.get('file')
        logger.info(f'Processing invoice: {file_path}')
        data = self.ocr.extract_text(file_path)
        return {'status': 'SUCCESS', 'output': {'data': data}, 'metrics': {'latency_ms': 120}}
