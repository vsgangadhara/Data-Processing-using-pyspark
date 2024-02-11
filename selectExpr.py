from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,monotonically_increasing_id,posexplode,lit,expr,concat
spark = SparkSession.builder \
    .appName("TransformDF") \
    .getOrCreate()
data = [("Bob", 16, ["Maths", "Physics", "Chemistry"], ["A", "B", "C"])]
df = spark.createDataFrame(data, ["Name", "Age", "Subjects", "Grades"])
df1=df.select(df.Name,df.Age,posexplode(df.Subjects).alias("pos","Subjects"))
df2=df.select(df.Name,df.Age,posexplode(df.Grades).alias("pos","grades"))
result=df1.join(df2,["Name","Age","pos"])

df4=result.select("*",lit("1").alias("temp"))
df5=df4.selectExpr("*","concat(grades,'+') as new_grade")
df5.show()