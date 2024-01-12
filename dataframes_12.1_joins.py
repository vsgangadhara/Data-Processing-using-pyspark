from pyspark import SparkConf
from pyspark.sql import SparkSession
my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
orders_df = spark.read\
.format("csv")\
.option("header",True)\
.option("inferSchema",True) \
.option("path","D:/BIGDATA/TrendyTech/week12/orders_1.csv") \
.load()


customers_df = spark.read\
.format("csv")\
.option("header",True)\
.option("inferSchema",True) \
.option("path","D:/BIGDATA/TrendyTech/week12/customers.csv") \
.load()

join_condition=orders_df.order_customer_id==customers_df.customer_id

joined_df=orders_df.join(customers_df,join_condition,"inner")

joined_df.show()
