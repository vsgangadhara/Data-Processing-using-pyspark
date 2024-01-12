from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import  *

# Create a Spark session
spark = SparkSession.builder.appName("RankRowNumberExample").getOrCreate()

# Sample data
data = [("Alice", 100),
        ("Bob", 150),
        ("Charlie", 120),
        ("David", 180),
        ("Eve", 130)]

# Define the schema
schema = ["name", "value"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Define a window specification
windowSpec = Window.orderBy(col("value").desc())

# Add rank, row number, and dense rank columns
#df = df.withColumn("rank", rank().over(windowSpec))
#df = df.withColumn("row_number", row_number().over(windowSpec))
#df = df.withColumn("dense_rank", dense_rank().over(windowSpec))
df1=df.withColumn("rank",rank().over(windowSpec))
df2=df.withColumn("dense_rank",dense_rank().over(windowSpec))
df3=df.withColumn("row_number",row_number().over(windowSpec))
df1.show()

my_window=Window.orderBy()
# Show the result
df.show()

# Stop the Spark session
spark.stop()
