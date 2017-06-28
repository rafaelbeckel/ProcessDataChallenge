import settings
from pymongo import MongoClient

class DB:
    """
    Provides functions for database connection and management
    """
    client = None
    database = None
    
    def __init__(self, **options):
        url = options.get('url', settings.MONGODB_URL)
        port = options.get('port', settings.MONGODB_PORT)
        database_name = options.get('database', settings.MONGODB_DATABASE)
        
        self.client = MongoClient(url, port)
        self.database = self.client[database_name]
        
        
    def reset(self):
        """
        Drops all collections from the database
        """
        db = self.database
        collections = db.collection_names(include_system_collections=False)
        for collection in collections:
            self.database[collection].drop()
        

    def hard_reset(self):
        """
        Drops the database
        """
        self.client.drop_database(self.database)