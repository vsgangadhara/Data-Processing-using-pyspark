from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
data=[(1,"ganga",31),(2,"priya",28),(2,"love",2)]
df1=spark.createDataFrame(data,("id","name","age"))

#df1=df1.filter(df1.age>28).show()
#df1=df1.filter(df1["age"]>28).show()
#df1=df1.filter(col("age")>28).show()

