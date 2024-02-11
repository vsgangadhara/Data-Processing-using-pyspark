from pyspark import SparkContext
import time
sc=SparkContext("local[*]","group_by_key")
data=["hello","world","hello","data","big","data","spark","pyspark","python","scala","python"]

rdd1=sc.parallelize(data)
rdd2=rdd1.map(lambda x:(x,1))
rdd3=rdd2.groupByKey()
rdd4=rdd3.map(lambda x:(x[0],len(x[1])))
for x in rdd4.collect():
    print(x)
