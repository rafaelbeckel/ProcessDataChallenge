import sys
import time
import settings
import multiprocessing as mp
 
from error import DelayedException
from pymongo import MongoClient
from datetime import datetime
from data import user #, product, order, cart, category

cpu_count = mp.cpu_count()

client = MongoClient(settings.MONGODB_URL, settings.MONGODB_PORT)
db = client[settings.MONGODB_DATABASE]


def generate_users(total_documents=30000000, batch_size=30000, workers=cpu_count):
    pool = mp.Pool(workers)
    start = datetime.now()
    documents_per_cpu = total_documents // workers

    try:
        for i in range(workers):
            pool.apply_async(_insert_users, [documents_per_cpu, batch_size])
    
    except DelayedException as e:
        e.re_raise() # get exceptions from children

    _report_inserts(total_documents)

    diff = str((datetime.now() - start).total_seconds())
    print('Created ' + str(total_documents) + ' users in ' + diff + 's')


def _insert_users(number, batch_size):
    child_client = MongoClient(settings.MONGODB_URL, settings.MONGODB_PORT)
    child_db = child_client[settings.MONGODB_DATABASE]

    while (number > 0):
        start = datetime.now()
        me = str(mp.current_process())

        try:
            users = user.generate(batch_size)
            child_db.users.insert_many(users)
            number = number - batch_size
            diff = str((datetime.now() - start).total_seconds())
            print('Worker ' + me + ' inserted ' + str(batch_size) + 
                  ' users in ' + diff + 's')

        except Exception as e:
            raise DelayedException(e) # raise exception to parent


def _report_inserts(number):
    inserted_documents = 0
    seconds = 0

    while (inserted_documents < number):
        inserted_documents = db.users.count()
        if (seconds >= 0 and seconds % 30 == 0):  
            print('We have ' + str(inserted_documents) + ' total users...')
        if (inserted_documents < number):
            seconds += 1
            time.sleep(1)