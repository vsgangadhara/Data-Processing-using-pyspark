from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("count").getOrCreate()
data=[("ganga",100)]
df1=spark.createDataFrame(data,("id","name"))
#df2=df1.toDF("id","name")
df2.show()
print(type(data))