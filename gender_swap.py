from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder.appName("GenderSwapExample").getOrCreate()

# Sample DataFrame
data = [("John", "Male"), ("Jane", "Female"), ("Sam", "Male"), ("Ella", "Female")]
columns = ["Name", "Gender"]
df = spark.createDataFrame(data, columns)

# Define a UDF for gender swap
@udf(StringType())
def swap_gender(gender):
    if gender.lower() == "male":
        return "Female"
    elif gender.lower() == "female":
        return "Male"
    else:
        return gender

# Apply the UDF to the DataFrame
df_result = df.withColumn("SwappedGender", swap_gender(df["Gender"]))

# Show the result
df_result.show()
