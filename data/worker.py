import settings
import importlib

from data.db import DB
from datetime import datetime
from multiprocessing import current_process


class Worker:
    """ 
    Inserts records of a given data model in the database
    """
    records = 0
    elapsed_time = 0
    required_arguments = ['name', 'payload', 'batch_size', 'database', 'model']
    
    def __init__(self, **options):
        self.__validate(options)
        self.name = options['name']
        self.payload = options['payload']
        self.remaining = options['payload']
        self.batch_size = options['batch_size']
        self.db = DB(database = options['database']).database
        self.__setup_model(options)
    
    
    def __validate(self, arguments):
        if not all(a in arguments for a in self.required_arguments):
            raise TypeError("Missing required arguments")
    
    
    def __setup_model(self, options):
        model_module = importlib.import_module('data.models.'+options['model'])
        model_class = getattr(model_module, options['model'].capitalize())
        self.model = model_class(self.db)
    
    
    def run(self):
        start = datetime.now()
        
        while (self.remaining > 0):
            self.run_batch()
        
        self.elapsed_time = (datetime.now() - start).seconds
    
    
    def run_batch(self):
        batch_start = datetime.now()
        
        if self.batch_size > self.remaining:
            self.batch_size = self.remaining
        
        records = self.model.generate(self.batch_size)
        self.model.insert_many(records)
        self.remaining -= self.batch_size
        
        batch_time = (datetime.now() - batch_start).seconds
        print(self.report(self.batch_size, batch_time))
    
    
    def report(self, records, time, action='inserted', suffix='...'):
        """ 
        Prints worker activity
        """
        return ('Worker ' + self.name + ' ' + action + ' ' + str(records) + ' '
                + self.model.collection + ' in ' + str(time) + 's' + suffix)


# Needs to be visible in global scope
def worker_job(*args):
    """ 
    Async function to insert records in the database.
    """
    try: 
        options = args[0]
        
        worker = Worker(
            name = current_process().name,
            model = options['model'],
            payload = options['payload'],
            database = options['database'],
            batch_size = options['batch_size']
        )
        
        worker.run()

    except Exception as e:
        return e # return exception as callback argument
    
    return worker.report(worker.payload, worker.elapsed_time, 
                         'has finished. It inserted a total of', '!')
    

def worker_success(message):
    """ 
    Worker success callback
    """
    print(message)
    


def worker_error(exception):
    """ 
    Worker error callback
    """
    print(str(exception))
