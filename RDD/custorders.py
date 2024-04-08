from pyspark import SparkContext
sc=SparkContext("local[*]","wordcount")
input=sc.textFile("D:/BIGDATA/TrendyTech/week9-spark1/dataset/moviedata.data")
rdd1=input.map(lambda x:(x.split("\t")[2],1))
rdd2=rdd1.reduceByKey(lambda x,y:x+y)
rdd3=rdd2.sortBy(lambda x:x[1],False)
result=rdd3.collect()

for a in result:
    print(a)