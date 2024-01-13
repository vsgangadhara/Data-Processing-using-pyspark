from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder.appName("EmployeeSalaryPivot").getOrCreate()

# Sample employee salary data
data = [("Alice", "Manager", 5000),
        ("Bob", "Developer", 6000),
        ("Charlie", "Manager", 5500),
        ("David", "Developer", 7000),
        ("Eve", "Analyst", 4500),
        ("Frank", "Analyst", 4800)]

# Define the schema
schema = ["name", "position", "salary"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Pivot the DataFrame
pivoted_df = df.groupBy("name").pivot("position").agg(first(col("salary")))
# Display the pivoted DataFrame
pivoted_df.show()
