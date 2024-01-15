from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.sql.types import StringType, StructType, StructField

# Create a Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([StructField("text", StringType(), True)])

# Create a sample DataFrame with a column containing text data
data = [("This is a sample sentence.",),
        ("Another sentence for word count.",),
        ("This is a third sentence.",)]

df = spark.createDataFrame(data, schema)

# Split the text into words and explode them into separate rows
df_words = df.select(explode(split(col("text"), " ")).alias("word"))

# Perform the word count
word_count = df_words.groupBy("word").count()

# Show the result
word_count.show()

# Stop the Spark session
spark.stop()
