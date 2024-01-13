from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder.appName("AccessColumnsExample").getOrCreate()

# Sample data
data = [("Alice", 25, "City1"),
        ("Bob", 30, "City2"),
        ("Charlie", 22, "City3")]

# Define the schema
schema = ["name", "age", "city"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Accessing columns using different methods
column1 = df.name  # Method 1
column2 = df.select("age")  # Method 2
column3 = df.select(col("city"))  # Method 3
column4 = df["age"]  # Method 4

# Display the results
column1.show()
column2.show()
column3.show()
column4.show()
