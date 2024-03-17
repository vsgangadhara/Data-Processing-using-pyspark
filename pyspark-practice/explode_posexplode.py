from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode, col
spark = SparkSession.builder \
 .appName("TransformDF") \
 .getOrCreate()
data = [("Bob", 16, ["Maths", "Physics", "Chemistry"], ["A", "B", "C"])]
df = spark.createDataFrame(data, ["Name", "Age", "Subjects", "Grades"])
df_subjects = df.select("Name", "Age", posexplode("Subjects").alias("pos", "Subject"))
df_grades = df.select("Name", "Age", posexplode("Grades").alias("pos", "Grade"))
result_df = df_subjects.join(df_grades, ["Name", "Age", "pos"])
result_df.show()