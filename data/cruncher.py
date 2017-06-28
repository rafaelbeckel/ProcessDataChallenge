import sys
import time
import settings
import multiprocessing as mp

from datetime import datetime
from data.worker import worker_job, worker_success, worker_error
from data.models.user import User
from data.models.cart import Cart
from data.models.order import Order
from data.models.product import Product


class Cruncher:
    """
    Crunches user shopping activity and generates activity collection
    """
    
    def __init__(self, database, **options):
        self.db = database
    
    
    def run():
        
        users = User(self.db)
        
        # let's create an intermediate table
        # joining carts, orders, products and categories
        db.carts.aggregate([
            {'$lookup' : {
                'from': 'orders',
                'localField': '_id',
                'foreignField': 'cart_id',
                'as': 'order'
            }},
            {'$unwind' : '$order'},
            {'$addFields' : { 
                'cart_id': '$order.cart_id',
                'date' : '$order.created_at',
                'month' : {'$month' : '$order.created_at'},
                'year' : {'$year' : '$order.created_at'}
            }},
            {'$unwind' : '$products'},
            {'$addFields' : {
                'product_id' : '$products.product_id',
                'product_quantity' : '$products.quantity',
                'product_price' : '$products.price'
            }},
            {'$lookup' : {
                'from': 'products',
                'localField': 'product_id',
                'foreignField': '_id',
                'as': 'product'
            }},
            {'$unwind' : '$product'},
            {'$project' : {
                '_id' : 0,
                'cart_id' : 1,
                'customer_id' : 1,
                'amount' : 1,
                'product_id' : 1,
                'product_quantity' : 1,
                'product_price' : 1,
                'product_categories' : '$product.categories',
                'date' : 1,
                'month' : 1,
                'year' : 1
            }},
            {'$out' : 'normalized_carts'}
        ])
        
        
        # A operação vai gerar esse documento:
        # {
        #     "_id" : ObjectId("595396a54f24114dce9ba819"),
        #     "amount" : 660.45,
        #     "customer_id" : ObjectId("595396a54f24114dce9ba817"),
        #     "cart_id" : ObjectId("595396a54f24114dce9ba819"),
        #     "date" : ISODate("2017-04-24T12:51:59Z"),
        #     "month" : 4,
        #     "year" : 2017,
        #     "product_id" : ObjectId("595396a24f24114dcf9ba7fe"),
        #     "product_quantity" : 4,
        #     "product_price" : 117.46,
        #     "product_categories" : [
        #             "Games",
        #             "Informática e Tablets",
        #             "Eletrodomésticos"
        #     ]
        # }
        
        
        
        
        
        db.users.aggregate([
            {'$lookup' : {
                'from': 'orders',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'normalized_carts'
            }},
            {'$project' : {
                'customer_id' : '$customer_id',
                'email' : '$email',
                'details' : {
                    'first_name' : '$first_name',
                    'last_name' : '$last_name'
                },
                
                #... $group ...
                
            },
            
            
            ])
            
            
        #     {'$project' : {
        #         'customer_id' : '$customer_id',
        #         'email' : '$email',
        #         'details' : {
        #             'first_name' : '$first_name',
        #             'last_name' : '$last_name'
        #         },
        #         'monthly_expenses' : {
        #             'jan' : '',
        #             'fev' : '',
        #             'mar' : '',
        #             'abr' : '',
        #             'mai' : '',
        #             'jun' : '',
        #             'jul' : '',
        #             'ago' : '',
        #             'set' : '',
        #             'out' : '',
        #             'nov' : '',
        #             'dev' : ''
        #         },
        #         'categorized_monthly_expenses' : {
        #             'jan' : {
        #                 'categoria_1' : 123.00,
        #                 'categoria_2' : 0.45
        #             },
        #             'fev' : '',
        #             'mar' : '',
        #             'abr' : '',
        #             'mai' : '',
        #             'jun' : '',
        #             'jul' : '',
        #             'ago' : '',
        #             'set' : '',
        #             'out' : '',
        #             'nov' : '',
        #             'dev' : ''
        #         },
        #         'monthly_avg_expense' : '',
        #         'categorized_monthly_avg_expense' : {
        #             'categoria_1' : 123.00,
        #             'categoria_2' : 0.45
        #         }
        #     }},
        # ],
        # {
        #     'allowDiskUse': 'true'
        # })
        
        # """
        # {
        #     "customer_id" : "58d549d3808c3c0b89263d51",
        #     "email" : "joselito@aol.com",
        #     "details" : {
        #         "first_name" : "joselito",
        #         "last_name" : "mesquita”
        #     },
        #     “monthly_spenses”: {
        #         “jan” : 123.45,
        #         “fev” : 678.90,
        #         “mar” : 0, 
        #         …
        #     }, 
        #     “categorized_monthly_expenses” : {
        #         “jan” : {  
        #             “categoria_1” : 123.00,
        #             “categoria_2” : 0.45
        #         },    
        #         …
        #     },
        #     “monthly_avg_expense” : 345.67,
        #     “categorized_monthly_avg_expense” : {
        #         “categoria_1” : 123.00,
        #         “categoria_2” : 0.45
        #         …
        #     }
        # """
        # pass
    
    
    