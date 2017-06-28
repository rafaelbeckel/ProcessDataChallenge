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
        
        #let's create an intermediate table
        #joining carts and orders
        db.carts.aggregate([
            {'$lookup' : {
                'from': 'orders',
                'localField': '_id',
                'foreignField': 'cart_id',
                'as': 'order'
            }},
            {'$unwind' : '$order'},
            {'$project' : {
                'customer_id' : '$customer_id',
                'amount' : '$amount',
                'products' : '$products',
                'date' : '$order.created_at',
                'month' : {'$month' : '$order.created_at'}
            }},
        ]).pretty()
        
        db.users.aggregate([
            {'$lookup' : {
                'from': 'orders',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'orders'
            }},
            {'$project' : {
                'customer_id' : '$customer_id',
                'email' : '$email',
                'details' : {
                    'first_name' : '$first_name',
                    'last_name' : '$last_name'
                },
                
            },
            {'$lookup' : {
                'from': 'carts',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'carts'
            }},
            {'$project' : {
                'customer_id' : '$customer_id',
                'email' : '$email',
                'details' : {
                    'first_name' : '$first_name',
                    'last_name' : '$last_name'
                },
                
            }
            
            ]).pretty()
            
            
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
    
    
    