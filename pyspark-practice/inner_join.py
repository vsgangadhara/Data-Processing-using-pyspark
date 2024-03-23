from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()

data=[("ganga",1,2000,10),("Priya",2,4000,20),("Latha",3,5000,30),("x",4,6000,40)]

dept=[(10,"BIG DATA"),(20,"PYSPARK"),(30,"PYTHON"),(40,"AZURE")]

df1=spark.createDataFrame(data,["name","eid","salary","dept_id"])
df2=spark.createDataFrame(dept,["dept_id","dept_name"])

joined_df=df1.join(df2,(df1.dept_id==df2.dept_id) & (df2.dept_name=="PYSPARK"),"inner")\
.select(df1["*"],df2.dept_name)

filter_df=joined_df.filter( (joined_df.salary>2000) & (joined_df.salary<=5000)).show()





