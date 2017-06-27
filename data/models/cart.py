import settings
from random import randint
from data.models.user import User
from data.models.model import Model
from data.models.product import Product

class Cart(Model):
    
    collection = 'carts'
    
    def generate(self, num):
        """
        A generator of shopping carts with reference to
        an existing user and containing some existing products.
        
        We select a random customer for each cart, so our shopping data will
        be distibuted in a organic bell-curved way, with some customers
        buying a lot and others who buy less or do not buy at all
        """
        for _ in range(num):
            amount = 0
            product_count = randint(1, settings.MAX_PRODUCTS_IN_CART)
            
            customer = list(User(self.db).aggregate([
                {'$project': {
                    'id' : '$customer_id',
                }},
                {'$sample': {'size': 1}}
            ]))[0]
            
            products = list(Product(self.db).aggregate([
                {'$project': {
                    'product_id' : '$_id',
                    'price' : '$price',
                    'quantity' : ''
                }},
                {'$sample': {'size': product_count}}
            ]))
            
            for product in products:
                product['quantity'] = randint(1, settings.MAX_PRODUCT_QUANTITY)
                amount += round(product['price'] * product['quantity'], 2)
            
            cart = {
                "customer_id" : customer['id'],
                "products" : products,
                "amount" : amount
            }
            
            yield cart