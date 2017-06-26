from random import randint
from data.models.user import User
from data.models.model import Model
from data.models.product import Product

class Cart(Model):
    
    collection = 'carts'
    customer_id = None
    
    def generate(self, num):
        """
        A generator of shopping carts with reference
        to a real user and containing some real products
        """
        for _ in range(num):
            amount = 0
            product_count = randint(1, 5)
            
            products = list(Product(self.db).aggregate([
                {'$project': {
                    'product_id' : '$_id',
                    'price' : '$price',
                    'quantity' : ''
                }},
                {'$sample': {'size': product_count}}
            ]))
            
            for product in products:
                product['quantity'] = randint(1, 5)
                amount += product['price'] * product['quantity']
            
            cart = {
                "customer_id" : self.customer_id,
                "products" : products,
                "amount" : amount
            }
            
            yield cart