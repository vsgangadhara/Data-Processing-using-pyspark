from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer
spark = SparkSession.builder \
    .appName("ExplodeVsExplodeOuter") \
    .getOrCreate()
data = [("John", ["apple", "banana", "orange"]),
        ("Alice", None),
        ("Bob", []),
        ("Jane", ["grape"])]
schema = ["name", "fruits"]
df = spark.createDataFrame(data, schema)
df_explode = df.select("name", explode(df.fruits).alias("fruit"))
df_explode_outer = df.select("name", explode_outer(df.fruits).alias("fruit"))
print("Using explode():")
df_explode.show()
print("Using explode_outer():")
df_explode_outer.show()

