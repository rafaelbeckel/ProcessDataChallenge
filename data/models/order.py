import settings
from faker import Faker
from random import randint
from data.models.user import User
from data.models.cart import Cart
from data.models.model import Model

class Order(Model):
    
    collection = 'orders'
    customer_id = None
    cart_id = None
    
    def generate(self, num):
        """
        Generates one order for a given cart/customer
        """
        fake = Faker(settings.FAKER_LANGUAGE)
        date = fake.date_time_between(settings.MAX_ORDER_AGE, 
                    end_date = "now"
                ).isoformat('T')
        
        return {
            'customer_id' : self.customer_id,
            'cart_id' : self.cart_id,
            'created_at' : "ISODate('" + date + "')"
        }