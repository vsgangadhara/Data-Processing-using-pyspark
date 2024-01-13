from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()
df = spark.read.format("csv")\
.option("inferSchema",True)\
.option("path","D:/BIGDATA/TrendyTech/week12/dataset1.csv")\
.load()
df1 = df.toDF("name","age","city")
def ageCheck(age):
    if(age>18):
        return "Y"
    else:
        return "N"

#spark.udf.register("parseAgeFunction",ageCheck,StringType())
parseAgeFunction = udf(ageCheck,StringType())
df2 = df1.withColumn("adult",parseAgeFunction("age"))
df1.show()
df1.printSchema()