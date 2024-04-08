from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel

import time

if __name__=="__main__":

    sc=SparkContext("local[*]","total amount")
    sc.setLogLevel("ERROR")
    input=sc.textFile("D:/BIGDATA/TrendyTech/week9-spark1/dataset/customerorders.csv")
    input_split=input.map(lambda x:(x.split(",")[0],float(x.split(",")[2])))
    reduce_input=input_split.reduceByKey(lambda x,y:x+y).cache()
    #reduce_input.cache()
    reduce_input.persist(StorageLevel.MEMORY_ONLY)
    #reduce_input.persist(StorageLevel.MEMORY_ONLY_2)
    #recue_input.persist(StorageLevel.MEMORY_ONLY_SER)
    #recuce_input.persist(StorageLevel.MEMORY_AND_DISK)
    #reduce_input.persist(StorageLevel.MEMORY_AND_DISK_2)
   # reduce_input.persist(StorageLevel.MEMORY_AND_DISK)
    reduce_input.unpersist()
   # reduce_input.persist(StorageLevel.OFF_HEAP)
   # reduce_input.persit(StorageLevel.DISK_ONLY)
   # reduce_input.persist(StorageLevel.DISK_ONLY_2)
    result=reduce_input.collect()
    count=reduce_input.cache()

    #count=reduce_input.persist(StorageLevel.MEMORY_ONLY)

    print(count)
    for i in result:
        print(i)
time.sleep(100)