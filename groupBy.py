from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("my app").getOrCreate()

data = [("John", 11599, 500),
        ("Alice", 11600, 600),
        ("Bob", 11599, 700)]

columns = ["name", "customerid", "amount"]
df1 = spark.createDataFrame(data, columns)

# Use where and groupBy together
df2 = df1.where("customerid = 11599")
df3 = df2.groupBy("customerid").agg({"customerid": "count"})

# Show the result directly without aggregation
df3.show()