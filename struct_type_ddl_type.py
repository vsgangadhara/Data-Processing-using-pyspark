from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark=SparkSession.builder.appName("my app").getOrCreate()

data=[(1,"ganga"),(2,"Priya"),(3,"Hemalatha")]

my_schema=StructType(
        [StructField("id",IntegerType(),False),
         StructField("name",StringType(),False)]
)
#my_ddl_schema="id Integer, name String"
#columns=["id","name"]
df1=spark.createDataFrame(data,columns)
df1.show()