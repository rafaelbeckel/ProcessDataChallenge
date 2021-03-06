import sys
import time
import settings
import multiprocessing as mp

from pymongo import DESCENDING
from datetime import datetime
from data.worker import worker_job, worker_success, worker_error
from data.models.draft import Draft


class Seeder:
    """
    Generates fake records in the database
    """
    
    def __init__(self, database, **options):
        self.db = database
        
        self.total_products = options.get('total_products', settings.PRODUCTS_COUNT)
        self.total_users = options.get('total_users', settings.USERS_COUNT)
        self.batch_size = options.get('batch_size', settings.BATCH_SIZE)
        self.workers = options.get('workers', mp.cpu_count())
        self.pool = mp.Pool(self.workers)
        
    
    def run(self):
        """ 
        Generates fake data
        """
        start = datetime.now()
        
        self._generate_products()
        self._generate_activity_drafts()
        
        self.drafts = Draft(self.db)
        self._generate_users()
        self._generate_carts()
        self._generate_orders()
        self.drafts.drop()
        
        self.elapsed_time = str((datetime.now() - start).seconds)
        self.print_status()
    
    
    def _generate_products(self):
        """ 
        Generates fake products.
        """
        payload = self.total_products // self.workers
        remainder = self.total_products % self.workers
        
        for _ in range(self.workers):
            self._start_worker('product', payload)
            
        if remainder:
            self._start_worker('product', remainder)
            
        self._report_inserts(['products'])
    
    
    def _generate_activity_drafts(self):
        """ 
        Generates a temporary collection that is used once. 
        It will be transformed into collections of users, carts and orders.
        """
        self.total_drafts = self.total_users
        payload = self.total_drafts // self.workers
        remainder = self.total_drafts % self.workers
        
        for _ in range(self.workers):
            self._start_worker('draft', payload)
            
        if remainder:
            self._start_worker('draft', remainder)
        
        self._report_inserts(['drafts'])
    
    
    def _start_worker(self, model, payload, **kwargs):
        """ 
        Starts a worker assyncronously.
        """
        self.pool.apply_async(worker_job, 
            args = [{
                'model' : model,
                'payload' : payload,
                'batch_size' : self.batch_size,
                'database' : self.db.name
            }],
            callback       = worker_success,
            error_callback = worker_error 
        )
        
        
    def _generate_users(self):
        """ 
        Generates user collection.
        """
        print('generating users collection...')
        
        self.drafts.aggregate([
            {'$project' : {
                'customer_id' : '$user_id',
                'email' : '$user_email',
                'details' : {
                    'first_name' : '$user_first_name',
                    'last_name' : '$user_last_name',
                    'full_name' : '$user_full_name'
                }
            }},
            {'$out' : 'users'}
        ])
        self.db.users.create_index('customer_id', unique=True)
        
        
    
    def _generate_carts(self):
        """ 
        Generates carts collection.
        """
        print('generating carts collection...')
        
        self.drafts.aggregate([
            {'$unwind' : '$carts'},
            {'$project': {
                '_id' : '$carts.cart_id',
                'customer_id' : '$user_id',
                'products' : '$carts.cart_products',
                'amount' : '$carts.cart_amount'
            }},
            {'$out' : 'carts'}
        ])
        self.db['carts'].create_index('customer_id')
        
        
    def _generate_orders(self):
        """ 
        Generates orders collection.
        """
        print('generating orders collection...')
        
        self.drafts.aggregate([
            {'$unwind' : '$carts'},
            {'$project': {
                '_id' : 0,
                'customer_id' : '$user_id',
                'cart_id' : '$carts.cart_id',
                'created_at' : '$carts.order_created_at'
            }},
            {'$out' : 'orders'}
        ])
        self.db['orders'].create_index('customer_id')
        self.db['orders'].create_index('cart_id')
        self.db['orders'].create_index([('created_at', DESCENDING)])
    
    
    def _report_inserts(self, collections):
        """ 
        Prints record count until we reach max number of inserts.
        It runs syncronously, so it'll prevent our program from 
        terminating before every record is inserted.
        """
        seconds = 0
        last_count = 0
        count = {c : 0 for c in collections}
        
        while any(count[c] < getattr(self, 'total_' + c) for c in collections):

            for c in collections:
                count[c] = self.db[c].count()
                
            if (seconds >= 0 and seconds % 10 == 0):
                if count[c] == last_count:
                    print('... still working ...') #@TODO progress bar
                else:
                    self.print_status()
                last_count = count[c]
                
            time.sleep(1)
            seconds += 1
        
        self.print_status()
        
    
    def print_status(self):
        print('')
        print('We have ' + str(self.db['products'].count()) + ' total products')
        print('We have ' + str(self.db['drafts'].count()) + ' draft records')
        print('We have ' + str(self.db['users'].count()) + ' total users')
        print('We have ' + str(self.db['carts'].count()) + ' total carts')
        print('We have ' + str(self.db['orders'].count()) + ' total orders')
        print('')
