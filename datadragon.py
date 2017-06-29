import ast
import click
import settings
import multiprocessing as mp

from data.db import DB
from pprint import pprint
from data.seeder import Seeder
from data.cruncher import Cruncher


db = DB()

@click.group(context_settings = dict(help_option_names = ['-h', '--help']))
def main():
    pass


@main.command()
@click.option( '--hard', is_flag=True,
               help = 'If passed, drops the db instead of its collections.' )
def reset(hard):
    """Drop all Collections so we can start again"""
    if hard:
        db.hard_reset()
        print("Dropped the database!")
    else:
        db.reset()
        print("Dropped all collections from the database!")
    

@main.command()
@click.option( '--users',    default = settings.USERS_COUNT, 
               help = 'Number of users to be inserted in the database.'    )
@click.option( '--products', default = settings.PRODUCTS_COUNT, 
               help = 'Number of products to be inserted in the database.' )
@click.option( '--batch',    default = settings.BATCH_SIZE, 
               help = 'Number of records to be created in each iteration.' )
@click.option( '--workers',  default = mp.cpu_count(), 
               help = 'Number of child processes to run insertion jobs.'   )
@click.option( '--reset',    is_flag=True,  
               help = 'Drop all records before inserting.'                 )
def generate(users, products, batch, workers, reset):
    """Inserts fake users, products and shopping activity data in MongoDB"""
    if (reset):
        reset(hard=False)
    
    seeder = Seeder(db.database, 
                    total_products = products, 
                    total_users = users,
                    batch_size = batch,
                    workers = workers)
    
    seeder.run()
    print('Finished in ' + seeder.elapsed_time + 's')


@main.command()
def crunch():
    """Crunches shopping activity and generates activity collection"""
    if (db.database['users'].count() and
        db.database['carts'].count() and
        db.database['orders'].count() and
        db.database['products'].count()):
        cruncher = Cruncher(db.database)
        cruncher.run()
        print('Finished in ' + cruncher.elapsed_time + 's')
        
    else:
        print('')
        print('You need to run "generate" command first')


@main.command()
@click.option( '--query',
               help = 'Queries the new table.')
def find(query):
    """Queries the activity table and print results"""
    if db.database['activity'].count():
        cursor = db.database['activity'].find( ast.literal_eval(query) )
        for document in cursor:
            print('_________________________________________')
            print('id: ' + str(document.get('customer_id')))
            print('E-mail: ' + document.get('email'))
            print('Full Name: ' + document.get('full_name'))
            print('Average Expenses (last 3 months): ' + str(round(document.get('average_monthly_expenses', 0),2)))
            print('')
            print('Monthly Expenses:')
            pprint(document.get('monthly_expenses'))
            print('')
            print('Monthly expenses / category:')
            pprint(document.get('categorized_monthly_expenses'))
            print('')
            print('Average monthly expenses / category (last 3 months):')
            pprint(document.get('categorized_monthly_expenses'))
            print('_________________________________________')
    
    else:
        print('')
        print('You need to run "generate" and "crunch" commands first')
