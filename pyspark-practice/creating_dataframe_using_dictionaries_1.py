from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Create a Pandas DataFrame
data = {'name': ['John', 'Alice', 'Bob'], 'age': [30, 25, 35]}

# Define schema for DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create PySpark DataFrame with explicit schema
df = spark.createDataFrame([(name, age) for name, age in zip(data['name'], data['age'])], schema=schema)

# Show the DataFrame
df.show()
