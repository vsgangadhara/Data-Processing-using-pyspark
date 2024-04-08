from pyspark import SparkContext
import time
sc=SparkContext("local[*]","group_by_key")
input=sc.textFile("D:/BIGDATA/TrendyTech/week-10-spark/dataset/biglog.txt")
map_rdd = input.map(lambda x: x.split(":"))
map_rdd1=map_rdd.map(lambda x:(x[0],x[1]))
grp_rdd=map_rdd1.groupByKey()
result_rdd=grp_rdd.map(lambda x:(x[0],len(x[1])))
result=result_rdd.collect()
url=sc._jsc.sc().uiWebUrl().get()
print(url)
for x in result:
    print(x)
time.sleep(100)

sc.stop()
