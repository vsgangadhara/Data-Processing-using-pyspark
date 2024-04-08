from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
data=[(1,"ganga",31),(2,"priya",28),(2,"love",29)]
df=spark.createDataFrame(data,("id","name","age"))

df1=df.filter(df.age>28).show()
df1=df.filter((df["age"]>28)&(df["age"]<=30)).show()
df1=df.filter((df["age"]>=28)|(df["age"]<=30))
df1=df.filter(df.age.isNull())
df1=df.filter(df.age.isNotNull())
df1=df.filter(df.name.like('p%')).show()
df1=df.filter(df.name.isin("ganga")).show()
df1=df.filter(df.name.contains('g')).show()
df1=df.filter(df.name.startswith('p')).show()
df1=df.filter(df.name.endswith('a')).show()
df1=df.filter(col("age")>28).show()

