import settings
from faker import Faker
from random import randint, choice, random
from data.models.model import Model
from data.models.category import categories

from data.models.model import Model

class Product(Model):
    collection = 'products'
    
    def generate(self, num):
        """
        A simple generator of fake products
        """
        fake = Faker(settings.FAKER_LANGUAGE)
        
        for _ in range(num):
            product = {
                'title' : fake.sentence(2),
                'description' : fake.sentence(50),
                'categories' : list(set(
                     [choice(categories) for _ in range(randint(1,5))]
                )),
                'price' : round(random()*settings.MAX_PRICE,2),
            }
            
            yield product