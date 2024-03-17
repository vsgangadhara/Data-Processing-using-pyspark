from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IdentifyAndRemoveDuplicates") \
    .getOrCreate()

# Define the DataFrame containing the metrics
data = [
    {"name": "metric1", "code": "code1", "value": "value1", "valueFormat": "format1"},
    {"name": "metric2", "code": "code2", "value": "value2", "valueFormat": "format2"},
    {"name": "metric1", "code": "code1", "value": "value1", "valueFormat": "format1"},  # Duplicate
    {"name": "metric3", "code": "code3", "value": "value3", "valueFormat": "format3"}
]

# Create DataFrame
df = spark.createDataFrame(data)

# Show DataFrame
print("DataFrame with Duplicates:")
df.show()

# Identify duplicates based on all columns
duplicates = df.groupBy(*df.columns).count().where(col("count") > 1)

if duplicates.count() > 0:
    print("Identified Duplicates:")
    duplicates.show()

    # Remove duplicates
    df_no_duplicates = df.dropDuplicates()

    # Show DataFrame after removing duplicates
    print("DataFrame after Removing Duplicates:")
    df_no_duplicates.show()
else:
    print("No duplicates found.")

# Stop SparkSession
spark.stop()
