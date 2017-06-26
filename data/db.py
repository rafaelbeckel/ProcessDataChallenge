import settings
from pymongo import MongoClient

class DB:
    """
    Provides functions for database connection and management
    """
    client = None
    database = None
    database_name = None
    
    def __init__(self, **options):
        self.url = options.get('url', settings.MONGODB_URL)
        self.port = options.get('port', settings.MONGODB_PORT)
        self.database_name = options.get('database', settings.MONGODB_DATABASE)
        
        self.client = MongoClient(self.url, self.port)
        self.database = self.client[self.database_name]
        
        
    def count(self, collection):
        """
        Counts total records of a given collection
        """
        return self.database[collection].count()
        
        
    def reset(self):
        """
        Drops all collections from the database
        """
        db = self.database
        db.activity.drop()
        db.orders.drop()
        db.carts.drop()
        db.products.drop()
        db.users.drop()
        

    def hard_reset(self):
        """
        Drops the database
        """
        self.client.drop_database(self.database)