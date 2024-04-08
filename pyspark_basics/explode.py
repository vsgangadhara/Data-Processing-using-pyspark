from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
spark = SparkSession.builder \
    .appName("TransformDF") \
    .getOrCreate()
data = [("Bob", 16, ["Maths", "Physics", "Chemistry"], ["A", "B", "C"])]
df = spark.createDataFrame(data, ["Name", "Age", "Subjects", "Grades"])
df_subjects = df.select("Name", "Age", explode(col("Subjects")).alias("Subject"))
df_grades = df.select("Name", "Age", explode(col("Grades")).alias("Grade"))
result_df = df_subjects.join(df_grades, ["Name", "Age"])

final=result_df.drop_duplicates(df_subjects.Name)
final.show()