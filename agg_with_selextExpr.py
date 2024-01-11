from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("my app").getOrCreate()

data=[("ganga",100),("priya",100),("latha",200),("ganga",50),("priya",50),("latha",80)]

df1=spark.createDataFrame(data,("name","marks"))
#df2=df1.select(count("name").alias("count"),sum("marks").alias("total marks"))

#df2=df1.selectExpr("count(*) as count","sum(marks) as total_marks","avg(marks) as avg_marks")

df2=df1.groupBy("name").agg(sum("marks").alias("total marks"),avg("marks").alias("average marks"),
#                            max("marks").alias("maxa marks"),min("marks").alias("Min Marks")
#                            ,countDistinct("name").alias("distinct names"))
df2.show()

