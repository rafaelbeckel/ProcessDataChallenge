import sys
import time
import settings
import multiprocessing as mp

from datetime import datetime
from data.models import user, product #, order, cart, category
from data.worker import worker_job, worker_success, worker_error


class Seeder:
    """
    Generates records in the database
    """
    verbose = False
    
    def __init__(self, database, **options):
        """ 
        Start all instance variables
        """
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
        
        self._start_workers()
        self._report_inserts()
        
        self._generate_ecommerce_activity()
        self._report_inserts()
        
        self.elapsed_time = str((datetime.now() - start).seconds)
    
    
    def _start_workers(self):
        """ 
        Delegates jobs to generate fake data.
        """
        user_load = self.total_users // self.workers
        product_load = self.total_products // self.workers
        
        for _ in range(self.workers):
            self._start_worker('user', user_load)
            self._start_worker('product', product_load)
    
    
    def _generate_ecommerce_activity(self):
        """ 
        Simulates user's activity in stores
        """
        return
    
    
    def _start_worker(self, model, worker_load, **kwargs):
        """ 
        Starts a worker assyncronously.
        """
        self.pool.apply_async(worker_job, 
            args = [{
                'model' : model,
                'payload' : worker_load,
                'batch_size' : self.batch_size,
                'database' : self.db.database_name
            }],
            callback       = worker_success,
            error_callback = worker_error 
        )
    
    
    def _report_inserts(self):
        """ Prints record count until we reach max number of inserts.
            It runs syncronously, so it'll prevent our program from 
            terminating before every record is inserted.
        """
        seconds = 0
        products = 0
        users = 0
        
        while (products < self.total_products or users < self.total_users):
            products = self.db.count('products')
            users = self.db.count('users')
            if (seconds >= 0 and seconds % 30 == 0):
                self._print_status()
            time.sleep(1)
            seconds += 1
        
        self._print_status()
            
    
    def _print_status(self):
        print('We have ' + str(self.db.count('products')) + ' total products')
        print('We have ' + str(self.db.count('users')) + ' total users')
