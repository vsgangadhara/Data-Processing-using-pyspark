from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("my app").getOrCreate()

data=[("ganga",100),("priya",100),("latha",200),("ganga",50),("priya",50),("latha",80)]

df1=spark.createDataFrame(data,("name","marks"))
df2=df1.select(count("name").alias("count"),sum("marks").alias("total marks"))
df2.show()
