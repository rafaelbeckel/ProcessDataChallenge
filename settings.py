# DB connection info
MONGODB_URL = 'localhost'
MONGODB_PORT = 27017
MONGODB_DATABASE = 'shopback'

# Language of generated data
FAKER_LANGUAGE = 'pt_BR'

# Number of generated customers
USERS_COUNT=800

# Number of generated products.
# Take care with this number, as
# the entire collection is stored
# on memory to optimize shopping
# activity generation
PRODUCTS_COUNT=80

# Max price of a single product
MAX_PRODUCT_PRICE=400

# Max orders of a single product
MAX_PRODUCT_QUANTITY=5

# Max number of products in cart
MAX_PRODUCTS_IN_CART=3

# Max orders for each customer
MAX_ORDERS_PER_USER=6

# How old can a order be
MAX_ORDER_AGE='-1y'

# Number of inserts per worker
BATCH_SIZE=100
