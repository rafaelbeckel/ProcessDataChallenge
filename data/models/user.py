import settings
from faker import Faker
from bson.objectid import ObjectId
from data.models.model import Model

class User(Model):
    collection = 'users'

    def generate(self, num):
        """
        A simple generator of fake users
        """
        fake = Faker(settings.FAKER_LANGUAGE)
    
        for _ in range(num):
            first_name = fake.first_name()
            last_name = fake.last_name()
    
            user = {
                'customer_id' : ObjectId(),
                'email' : fake.email(),
                'details' : {
                    'first_name' : first_name,
                    'last_name' : last_name,
                    'full_name' : first_name + ' ' + last_name
                }
            }
    
            yield user