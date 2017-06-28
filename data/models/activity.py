import settings
from faker import Faker
from random import randint, random, sample
from bson.objectid import ObjectId
from data.models.model import Model
from data.models.product import Product
from data.models.category import categories


class Activity(Model):
    """
    An aggregated table of user activity for reporting purposes
    """
    collection = 'activities'
    
    def generate(self, num):
        pass