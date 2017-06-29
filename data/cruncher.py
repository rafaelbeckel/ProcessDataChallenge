from pymongo import DESCENDING
from datetime import datetime, date, timedelta


class Cruncher:
    """
    Crunches user shopping activity and generates activity collection
    """
    
    def __init__(self, database, **options):
        self.db = database
    
    
    def run(self):
        start = datetime.now()
        
        # @TODO organizar as queries abaixo em métodos separados
        
        # let's create an intermediate table
        # joining carts, orders, products and categories
        self.db['carts'].aggregate([
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
            {'$out' : 'denormalized_carts'}
        ])
        self.db['denormalized_carts'].create_index([('date', DESCENDING)])
        self.db['denormalized_carts'].create_index([('year', DESCENDING)])
        self.db['denormalized_carts'].create_index([('month', DESCENDING)])
        self.db['denormalized_carts'].create_index('cart_id')
        self.db['denormalized_carts'].create_index('customer_id')
        
        
        # Monthly Expenses
        self.db['denormalized_carts'].aggregate([
            {'$group' : {
                '_id' : '$cart_id',
                'date' : { '$first' : '$date' },
                'year' : { '$first' : '$year' },
                'month' : { '$first' : '$month' },
                'amount' : { '$first' : '$amount' },
                'customer_id' : { '$first' : '$customer_id' }
            }},
            {'$group' : {
                '_id' : { 'year' : '$year', 
                          'month' : '$month', 
                          'customer_id' : '$customer_id' },
                'amount' : { '$sum' : '$amount' },
                'date' : { '$first' : '$date' }
            }},
            {'$project' : {
                '_id' : 0,
                'date' : '$date',
                'year' : '$_id.year',
                'month' : '$_id.month',
                'customer_id' : '$_id.customer_id',
                'amount' : '$amount'
            }},
            {'$out' : 'monthly_expenses'}
        ])
        self.db['monthly_expenses'].create_index([('date', DESCENDING)])
        self.db['monthly_expenses'].create_index([('year', DESCENDING)])
        self.db['monthly_expenses'].create_index([('month', DESCENDING)])
        self.db['monthly_expenses'].create_index('customer_id')
        
        
        # Categorized Monthly Expenses
        self.db['denormalized_carts'].aggregate([
            {'$project' : {
                'date' : 1,
                'year' : 1,
                'month' : 1,
                'customer_id' : 1,
                'category' : '$product_categories',
                'category_amount' : { '$multiply' : [ '$product_price', '$product_quantity' ]}
            }},
            {'$unwind' : '$category'},
            {'$group' : {
                '_id' : { 'customer_id' : '$customer_id',
                          'category' : '$category',
                          'year' : '$year',
                          'month' : '$month' },
                'category_amount' : { '$sum' : '$category_amount'},
                'date' : { '$first' : '$date' }
            }},
            {'$project' : {
                '_id' : 0,
                'date' : '$date',
                'year' : '$_id.year',
                'month' : '$_id.month',
                'customer_id' : '$_id.customer_id',
                'category' : '$_id.category',
                'amount' : '$category_amount'
            }},
            {'$out' : 'categorized_monthly_expenses'}
        ])
        self.db['categorized_monthly_expenses'].create_index([('date', DESCENDING)])
        self.db['categorized_monthly_expenses'].create_index([('year', DESCENDING)])
        self.db['categorized_monthly_expenses'].create_index([('month', DESCENDING)])
        self.db['categorized_monthly_expenses'].create_index('customer_id')
           
        
        # Average last three months
        this_month = date.today()
        three_months = timedelta(3 * 365/12)
        three_months_ago = (this_month - three_months).replace(day = 1)
        three_months_ago = datetime.combine(three_months_ago, datetime.min.time())
        
        self.db['monthly_expenses'].aggregate([
            {'$match' : { 'date' : { '$gt' : three_months_ago } }},
            {'$group' : {
                '_id' : '$customer_id',
                'amount' : { '$sum' : '$amount' }
            }},
            {'$project' : {
                '_id' : 0,
                'customer_id' : '$_id',
                'amount' : { '$divide' : [ '$amount', 3 ] }
            }},
            {'$out' : 'average_monthly_expenses'}
        ])
        self.db['average_monthly_expenses'].create_index('customer_id')
        
        
        # Average last three months per category
        self.db['categorized_monthly_expenses'].aggregate([
            {'$match' : { 'date' : { '$gt' : three_months_ago } }},
            {'$group' : {
                '_id' : { 'customer_id' : '$customer_id',
                          'category' : '$category' },
                'amount' : { '$sum' : '$amount' }
            }},
            {'$project' : {
                '_id' : 0,
                'customer_id' : '$_id.customer_id',
                'category' : '$_id.category',
                'amount' : { '$divide' : [ '$amount', 3 ] }
            }},
            {'$out' : 'categorized_average_monthly_expenses'}
        ])
        self.db['categorized_average_monthly_expenses'].create_index('customer_id')
        
        
        # Generate Activity Collection
        self.db['users'].aggregate([
            {'$lookup' : {
                'from': 'monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'monthly_expenses'
            }},
            {'$lookup' : {
                'from': 'categorized_monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'categorized_monthly_expenses'
            }},
            {'$lookup' : {
                'from': 'average_monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'average_monthly_expenses'
            }},
            {'$lookup' : {
                'from': 'categorized_average_monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'categorized_average_monthly_expenses'
            }},
            {'$project' : {
                'email' : 1,
                'customer_id' : 1,
                'full_name' : '$details.full_name',
                'monthly_expenses.year' : 1,
                'monthly_expenses.month' : 1,
                'monthly_expenses.amount' : 1,
                'categorized_monthly_expenses.year' : 1,
                'categorized_monthly_expenses.month' : 1,
                'categorized_monthly_expenses.amount' : 1,
                'categorized_monthly_expenses.category' : 1,
                'average_monthly_expenses' : {'$arrayElemAt' : ['$average_monthly_expenses.amount', 0]},
                'categorized_average_monthly_expenses.amount' : 1,
                'categorized_average_monthly_expenses.category' : 1
            }},
            {'$out' : 'activity'}
        ])
        self.db['activity'].create_index('customer_id')
        
        
        self.elapsed_time = str((datetime.now() - start).seconds)