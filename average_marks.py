from pyspark import SparkContext
sc=SparkContext("local[*]","Average Marks")
input_data=[('A',100),('B',55),('C',88),('A',99),('B',55),('C',100),('A',66),('B',88),('C',77)]
input_rdd=sc.parallelize(input_data)
#grouped_data=input_rdd.groupByKey()
map_rdd=input_rdd.mapValues(lambda x:(x,1))
reduce_rdd=map_rdd.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
average=reduce_rdd.mapValues(lambda x:round(x[0]/x[1],2)).sortBy(lambda x:x[0])
for x in average.collect():
    print(x)