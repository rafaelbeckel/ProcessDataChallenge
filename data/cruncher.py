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
        
        self._denormalize_carts()
        self._crunch_monthly_expenses()
        self._crunch_average_monthly_expenses()
        self._crunch_categorized_monthly_expenses()
        self._crunch_categorized_average_monthly_expenses()
        self._create_activity_collection()
        
        self.elapsed_time = str((datetime.now() - start).seconds)
        
        
    def _denormalize_carts(self):
        """
        join carts, orders, products and categories
        """
        print('Denormalizing carts...')
        
        self.db['carts'].aggregate([
            
            # Join 'orders' collection
            {'$lookup' : {
                'from': 'orders',
                'localField': '_id',
                'foreignField': 'cart_id',
                'as': 'order'
            }},
            
            # Denormalize collection by order
            {'$unwind' : '$order'},
            
            # Add cart_id, month and year fields 
            {'$addFields' : { 
                'cart_id': '$order.cart_id',
                'date' : '$order.created_at',
                'month' : {'$month' : '$order.created_at'},
                'year' : {'$year' : '$order.created_at'}
            }},
            
            # Denormalize collection by cart products
            # Collection will contain duplicated carts
            {'$unwind' : '$products'},
            
            # Copy cart products data to the root of the document
            {'$addFields' : {
                'product_id' : '$products.product_id',
                'product_quantity' : '$products.quantity',
                'product_price' : '$products.price'
            }},
            
            # Join 'products' collection to get their categories
            {'$lookup' : {
                'from': 'products',
                'localField': 'product_id',
                'foreignField': '_id',
                'as': 'product'
            }},
            
            # Denormalize collection by all products
            {'$unwind' : '$product'},
            
            # Reshape document
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
            
            # Export new collection
            {'$out' : 'denormalized_carts'}
            
        ])
        self.db['denormalized_carts'].create_index([('date', DESCENDING)])
        self.db['denormalized_carts'].create_index([('year', DESCENDING)])
        self.db['denormalized_carts'].create_index([('month', DESCENDING)])
        self.db['denormalized_carts'].create_index('cart_id')
        self.db['denormalized_carts'].create_index('customer_id')
        
        
    def _crunch_monthly_expenses(self):
        """
        Creates monthly expenses collection
        """
        print('Crunching monthly expenses...')
        
        self.db['denormalized_carts'].aggregate([
            
            # Regroup duplicated carts
            {'$group' : {
                '_id' : '$cart_id',
                'date' : { '$first' : '$date' },
                'year' : { '$first' : '$year' },
                'month' : { '$first' : '$month' },
                'amount' : { '$first' : '$amount' },
                'customer_id' : { '$first' : '$customer_id' }
            }},
            
            # Group carts by user/month and sum their amounts
            {'$group' : {
                '_id' : { 'year' : '$year', 
                          'month' : '$month', 
                          'customer_id' : '$customer_id' },
                'amount' : { '$sum' : '$amount' },
                'date' : { '$first' : '$date' }
            }},
            
            # Reshape document
            {'$project' : {
                '_id' : 0,
                'date' : 1,
                'year' : '$_id.year',
                'month' : '$_id.month',
                'customer_id' : '$_id.customer_id',
                'amount' : '$amount'
            }},
            
            # Export new collection
            {'$out' : 'monthly_expenses'}
            
        ])
        self.db['monthly_expenses'].create_index([('date', DESCENDING)])
        self.db['monthly_expenses'].create_index([('year', DESCENDING)])
        self.db['monthly_expenses'].create_index([('month', DESCENDING)])
        self.db['monthly_expenses'].create_index('customer_id')
    
    
    def _crunch_average_monthly_expenses(self):
        """
        Creates average monthly expenses collection
        """
        print('Crunching average monthly expenses...')
        
        three_months_ago = self._three_months_ago()
        
        self.db['monthly_expenses'].aggregate([

            # Filter last three months
            {'$match' : { 'date' : { '$gt' : three_months_ago } }},

            # Group by customer id and sum amount
            {'$group' : {
                '_id' : '$customer_id',
                'amount' : { '$sum' : '$amount' }
            }},

            # Reshape document
            {'$project' : {
                '_id' : 0,
                'customer_id' : '$_id',
                'amount' : { '$divide' : [ '$amount', 3 ] }
            }},

            # Export new collection
            {'$out' : 'average_monthly_expenses'}

        ])
        self.db['average_monthly_expenses'].create_index('customer_id')
    
    
    def _crunch_categorized_monthly_expenses(self):
        """
        Creates categorized monthly expenses collection
        """
        print('Crunching categorized monthly expenses...')
        
        self.db['denormalized_carts'].aggregate([
            
            # Simplify document and calculate total spent in category
            {'$project' : {
                'date' : 1,
                'year' : 1,
                'month' : 1,
                'customer_id' : 1,
                'category' : '$product_categories',
                'category_amount' : { '$multiply' : [ '$product_price', '$product_quantity' ]}
            }},
            
            # Denormalize by category array
            {'$unwind' : '$category'},
            
            # Regroup categories by month/customer and sum their amounts
            {'$group' : {
                '_id' : { 'customer_id' : '$customer_id',
                          'category' : '$category',
                          'year' : '$year',
                          'month' : '$month' },
                'category_amount' : { '$sum' : '$category_amount'},
                'date' : { '$first' : '$date' }
            }},
            
            # Reshape document
            {'$project' : {
                '_id' : 0,
                'date' : '$date',
                'year' : '$_id.year',
                'month' : '$_id.month',
                'customer_id' : '$_id.customer_id',
                'category' : '$_id.category',
                'amount' : '$category_amount'
            }},
            
            # Export new collection
            {'$out' : 'categorized_monthly_expenses'}
            
        ])
        self.db['categorized_monthly_expenses'].create_index([('date', DESCENDING)])
        self.db['categorized_monthly_expenses'].create_index([('year', DESCENDING)])
        self.db['categorized_monthly_expenses'].create_index([('month', DESCENDING)])
        self.db['categorized_monthly_expenses'].create_index('customer_id')
    
    
    def _crunch_categorized_average_monthly_expenses(self):
        """
        Creates categorized average monthly expenses collection
        """
        print('Crunching categorized average monthly expenses...')
        
        three_months_ago = self._three_months_ago()
        
        # Average last three months per category
        self.db['categorized_monthly_expenses'].aggregate([
            
            # Filter last three months
            {'$match' : { 'date' : { '$gt' : three_months_ago } }},
            
            # Group by category/customer id and sum amount
            {'$group' : {
                '_id' : { 'customer_id' : '$customer_id',
                          'category' : '$category' },
                'amount' : { '$sum' : '$amount' }
            }},
            
            # Reshape document
            {'$project' : {
                '_id' : 0,
                'customer_id' : '$_id.customer_id',
                'category' : '$_id.category',
                'amount' : { '$divide' : [ '$amount', 3 ] }
            }},
            
            # Export new collection
            {'$out' : 'categorized_average_monthly_expenses'}
            
        ])
        self.db['categorized_average_monthly_expenses'].create_index('customer_id')
    
    
    def _create_activity_collection(self):
        """
        Creates activity collection
        """
        print('Creating activity collection...')
        
        self.db['users'].aggregate([
            
            # Join monthly_expenses collection
            {'$lookup' : {
                'from': 'monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'monthly_expenses'
            }},
            
            # Join categorized_monthly_expenses collection
            {'$lookup' : {
                'from': 'categorized_monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'categorized_monthly_expenses'
            }},
            
            # Join average_monthly_expenses collection
            {'$lookup' : {
                'from': 'average_monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'average_monthly_expenses'
            }},
            
            # Join categorized_average_monthly_expenses collection
            {'$lookup' : {
                'from': 'categorized_average_monthly_expenses',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'categorized_average_monthly_expenses'
            }},
            
            # Reshape our beautiful final document
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
            
            # Export our final document
            {'$out' : 'activity'}
            
        ])
        self.db['activity'].create_index('customer_id')
    
    
    def _three_months_ago(self):
        this_month = date.today()
        three_months = timedelta(3 * 365/12)
        three_months_ago = (this_month - three_months).replace(day = 1)
        return datetime.combine(three_months_ago, datetime.min.time())
