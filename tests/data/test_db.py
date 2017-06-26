import settings
from data.db import DB
from unittest import TestCase
from data.seeder import Seeder
from multiprocessing import cpu_count

class TestDB(TestCase):
    
    test_database = 'test_'+settings.MONGODB_DATABASE
    db = DB(database = test_database)

    def setUp(self):
        self.db = DB(mongodb_database = self.test_database)
    
    
    def tearDown(self):
        self.db.hard_reset()
    
    
    def test_constructor_defaults(self):
        seeder = Seeder(self.db)
        self.assertEqual( seeder.total_users, settings.USERS_COUNT )
        self.assertEqual( seeder.total_products, settings.PRODUCTS_COUNT )
        self.assertEqual( seeder.batch_size, settings.BATCH_SIZE )
        self.assertEqual( seeder.workers, cpu_count() )
        
        
    def test_constructor_options(self):
        seeder = Seeder(self.db,
            total_users = 100,
            total_products = 100,
            batch_size = 10,
            workers = 2
        )
        
        self.assertEqual( seeder.total_users, 100 )
        self.assertEqual( seeder.total_products, 100 )
        self.assertEqual( seeder.batch_size, 10 )
        self.assertEqual( seeder.workers, 2 )