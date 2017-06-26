import click
import settings
import multiprocessing as mp

from data.db import DB
from data.seeder import Seeder


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
@click.option( '-u', '--users',    default = settings.USERS_COUNT, 
               help = 'Number of users to be inserted in the database.'    )
@click.option( '-p', '--products', default = settings.PRODUCTS_COUNT, 
               help = 'Number of products to be inserted in the database.' )
@click.option( '-b', '--batch',    default = settings.BATCH_SIZE, 
               help = 'Number of records to be created in each iteration.' )
@click.option( '-w', '--workers',  default = mp.cpu_count(), 
               help = 'Number of child processes to run insertion jobs.'   )
@click.option( '-r', '--reset',    is_flag=True,  
               help = 'Drop all records before inserting.'                 )
@click.option( '-v', '--verbose',  is_flag=True,  
               help = 'Drop all records before inserting.'                 )
def generate(users, products, batch, workers, reset, verbose):
    """Inserts fake users, products and shopping activity data in MongoDB"""
    if (reset):
        reset(hard=False)
        
    seeder = Seeder(db, 
                    total_products = products, 
                    total_users = users,
                    batch_size = batch,
                    workers = workers)
    
    seeder.verbose = 
    seeder.run()
    print('Finished in ' + seeder.elapsed_time + 's')


@main.command()
def crunch():
    """Generates and crunches some data in MongoDB"""
    print("Hi!")


@main.command()
def report():
    """Queries the generated table"""
    print("Hi!")