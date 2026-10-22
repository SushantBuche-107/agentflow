
class VectorStore:
    def __init__(self):
        self.docs = {'doc1.txt': 'Sample doc for RAG'}
    def search(self, query):
        return [{'doc': 'doc1.txt', 'score': 0.95, 'content': self.docs['doc1.txt']}]
