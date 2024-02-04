from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Create a SparkSession
spark = SparkSession.builder.appName("TransformDF").getOrCreate()

# Create the input DataFrame
data = [("Bob", 16, ["Maths", "Physics", "Chemistry"], ["A", "B", "C"])]
df = spark.createDataFrame(data, ["Name", "Age", "Subjects", "Grades"])

# Explode Subjects and Grades arrays, retaining necessary columns
df = df.select("Name", "Age", explode(col("Subjects")).alias("Subject"), "Grades") \
       .select("Name", "Age", "Subject", explode(col("Grades")).alias("Grade"))

# Display the output DataFrame
df.show()