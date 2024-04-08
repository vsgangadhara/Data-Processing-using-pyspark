from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
spark = SparkSession.builder \
    .appName("IdentifyDuplicates") \
    .getOrCreate()

# Sample dictionary
data_dict = {
    "metrics": [
        {
            "name": "metric1",
            "code": "code1",
            "value": "value1",
            "valueFormat": "format1"
        },
        {
            "name": "metric2",
            "code": "code2",
            "value": "value2",
            "valueFormat": "format2"
        },
        {
            "name": "metric1",
            "code": "code1",
            "value": "value1",
            "valueFormat": "format1"  # Duplicate
        }
    ]
}

metrics_list = data_dict["metrics"]

df = spark.createDataFrame(metrics_list)
print("DataFrame:")
df.show()
duplicates = df.groupBy(*df.columns).agg(count("*").alias("count"))
duplicates = duplicates.filter(col("count") > 1)
if duplicates.count() > 0:
    print("Identified Duplicates:")
    duplicates.show()
else:
    print("No duplicates found.")
spark.stop()
