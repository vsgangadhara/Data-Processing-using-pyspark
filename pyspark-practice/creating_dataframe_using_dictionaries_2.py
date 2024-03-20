from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
data = [{'name': 'John', 'age': 30}, {'name': 'Alice', 'age': 25}, {'name': 'Bob', 'age': 35}]
df = spark.createDataFrame(data)
df.show()