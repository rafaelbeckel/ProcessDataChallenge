import settings
from faker import Faker
from random import randint, random, sample
from bson.objectid import ObjectId
from data.models.model import Model
from data.models.product import Product
from data.models.category import categories


class Draft(Model):
    """
    A temporary table that is generated once. It will be transformed 
    into collections of users, carts and orders using MongoDB's
    native aggregation framework
    """
    collection = 'drafts'
    
    orders_per_user = settings.MAX_ORDERS_PER_USER
    
    def __init__(self, db=None):
        super().__init__(db)
        self.products = list(Product(self.db).aggregate([
            {'$project': {
                '_id' : 0,
                'product_id' : '$_id',
                'price' : '$price'
            }},
        ]))
    
    
    def generate(self, num):
        """
        A simple generator of e-commerce activity
        """
        fake = Faker(settings.FAKER_LANGUAGE)
        
        for _ in range(num):
            first_name = fake.first_name()
            last_name = fake.last_name()
            user_id = ObjectId()
            
            carts = self._generate_carts(user_id)
            
            activity = {
                'user_id' : user_id,
                'user_email' : fake.email(),
                'user_first_name' : first_name,
                'user_last_name' : last_name,
                'user_full_name' : first_name + ' ' + last_name,
                'carts' : carts
            }
            
            yield activity
            
            
    def _generate_carts(self, user_id):
        """
        Generates random shooping activity for a given user
        """
        carts = []
        orders_per_user = randint(1, self.orders_per_user)
        
        for _ in range(orders_per_user):
            products = self._generate_products()
            amount = sum(p['price'] * p['quantity'] for p in products)
            
            carts.append({
                'cart_id' : ObjectId(),
                'cart_products' : products,
                'cart_amount' : round(amount, 2),
                'order_created_at' : self._random_date()
            })
        
        return carts
        
        
    def _generate_products(self):
        """
        Generates random products in cart
        """
        product_count = randint(1, settings.MAX_PRODUCTS_IN_CART)
        selected_products = sample(self.products, product_count)
        
        products = []
        for i, selected in enumerate(selected_products):
            product = {}
            product['product_id'] = selected['product_id']
            product['price'] = selected['price']
            product['quantity'] = randint(1, settings.MAX_PRODUCT_QUANTITY)
            products.append(product)
        
        return products
    
    
    def _random_date(self):
        fake = Faker(settings.FAKER_LANGUAGE)
        return fake.date_time_between(
            start_date = settings.MAX_ORDER_AGE, 
            end_date = 'now'
        )