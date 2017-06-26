from random import randint
from data.models.user import User
from data.models.cart import Cart
from data.models.model import Model

class Order(Model):
    
    collection = 'orders'
    
    def generate(self, num):
        """
        A generator of orders referencing a shopping cart
        """
        for _ in range(num):
            amount = 0
            product_count = randint(1, 5)
            
            
            for product in products:
                product['quantity'] = randint(1, 5)
                amount += product['price'] * product['quantity']
            
            order = {
                "customer_id" : "58812e310afc660dbe1d0982",
                "cart_id" : "58812eaa0afc660dc06a2278",
                "created_at" : ISODate("2017-01-19T21:25:59.000Z")
            }
            
            yield cart