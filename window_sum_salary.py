from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, sum

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data
data = [("Alice", "HR", 5000),
        ("Bob", "Engineering", 6000),
        ("Charlie", "HR", 4500),
        ("David", "Engineering", 7000),
        ("Eva", "HR", 5500)]

# Define the schema
schema = ["Name", "Department", "Salary"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=schema)

# Define a window specification based on the "Department" column
window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())
  #                 .rowsBetween(Window.unboundedPreceding, Window.currentRow)

Window.partitionBy()
Window.partitionBy()
Window.partitionBy("Department")

my_window=Window.partitionBy("Department").orderBy(col("salary").desc())

my_sal=df.withColumn("cummulative sal",sum(col("salary")).over(my_window))

my_sal.show()

# Add a cumulative salary column using the sum window function
#df_with_cumulative_salary = df.withColumn("cumulative_salary", sum(col("Salary")).over(window_spec))

# Show the DataFrame with cumulative salary
#df_with_cumulative_salary.show()
