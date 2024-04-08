from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead
spark = SparkSession.builder \
    .appName("CalculateSales") \
    .getOrCreate()
data = [
    ("TV", "2016-11-27", 800),
    ("TV", "2016-11-30", 900),
    ("TV", "2016-12-29", 500),
    ("FRIDGE", "2016-10-11", 760),
    ("FRIDGE", "2016-10-13", 400)
]
df = spark.createDataFrame(data, ["PRODUCT", "SALE_DATE", "AMOUNT"])
df = df.withColumn("SALE_DATE", col("SALE_DATE").cast("date"))
window_spec = Window.partitionBy("PRODUCT").orderBy("SALE_DATE")
df = df.withColumn("PREVIOUS_DAY_SALE", lag("AMOUNT").over(window_spec)) \
       .withColumn("NEXT_DAY_SALE", lead("AMOUNT").over(window_spec))
print("DataFrame with calculated sales:")
df.show()
spark.stop()
