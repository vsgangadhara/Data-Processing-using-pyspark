from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Masking Example") \
    .getOrCreate()

# Data
data = [
  (1, "Rahul", "1234567891234567"),
  (2, "Raj", "1004567892345678"),
  (3, "Priya", "0234567893456789"),
  (4, "Murti", "2234567890123456")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "card_number"])

# Masking logic
masked_df = df.withColumn("masked_card_number", expr("substring(card_number, 1, 4) || '********' || substring(card_number, -4)"))

# Show output
masked_df.select("id", "name", "masked_card_number").show(truncate=False)

# Stop SparkSession
spark.stop()
