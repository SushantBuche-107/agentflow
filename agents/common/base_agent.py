
class BaseAgent:
    def __init__(self, context):
        self.context = context

    def run(self, payload: dict) -> dict:
        return {'status': 'SUCCESS', 'output': {}, 'metrics': {}}
