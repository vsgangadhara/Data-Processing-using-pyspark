from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,monotonically_increasing_id
spark = SparkSession.builder \
    .appName("TransformDF") \
    .getOrCreate()
data = [("Bob", 16, ["Maths", "Physics", "Chemistry"], ["A", "B", "C"])]
df = spark.createDataFrame(data, ["Name", "Age", "Subjects", "Grades"])
df_subjects = df.select("Name", "Age", explode(col("Subjects")).alias("Subject"),monotonically_increasing_id().alias("id"))
df_grades = df.select("Name", "Age", explode(col("Grades")).alias("Grade"),monotonically_increasing_id().alias("id"))
#result_df = df_subjects.join(df_grades, ["Name", "Age"])
df1=df.withColumn("id",monotonically_increasing_id()).show()
#result_df.show()
df_subjects.show()
df_grades.show()