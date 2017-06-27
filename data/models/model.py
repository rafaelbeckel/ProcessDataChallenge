class Model:
    db = None
    collection = None
    
    def __init__(self, db=None):
        self.db = db
        
    def count(self):
        return self.db[self.collection].count()
        
    def insert(self, document):
        self.db[self.collection].insert(document)
        
    def insert_many(self, records):
        self.db[self.collection].insert_many(records)
    
    def find(self, query):
        return self.db[self.collection].find(
            query
        )
        
    def aggregate(self, pipeline):
        return self.db[self.collection].aggregate(
            pipeline
        )
        
    def drop(self):
        self.db[self.collection].drop()