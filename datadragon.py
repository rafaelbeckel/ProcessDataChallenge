import click

from data import seeder

#@click.option('--users')
#@click.option('--')
@click.command()
def main():
    '''This script generates and crunches some data in MongoDB'''
    seeder.generate_users()
