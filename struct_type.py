from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark=SparkSession.builder.appName("count").getOrCreate()
my_shcema=StructType([
    StructField("id", StringType(), False),
    StructField("name", IntegerType(), False)
])

StructType()
data=[("ganga",100)]
df1=spark.createDataFrame(data,my_shcema)
#df2=df1.toDF("id","name")
df1.show()
print(type(data))



