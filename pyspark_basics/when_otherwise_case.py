from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Dave", 40)]

df=spark.createDataFrame(data,["name","age"])
# Applying case statement
result=df.withColumn("age_group",when(col("age")<30,"young")
                     .when((col("age")>=30) & (col("age")<40),"mid age")
                .otherwise("old"))

result2=df.withColumn("age_group2",expr("case when age< 30 then 'young' "+
                                        "when age>=30 and age<40 then 'midd_age'"+
                                        "else 'old' end"
                                        ))

result2.show()

