from pyspark import SparkContext
sc=SparkContext("local[*]","Average Marks")
input_data=[('A',100),('B',55),('C',88),('A',99),('B',55),('C',100),('A',66),('B',88),('C',77)]
input_rdd=sc.parallelize(input_data)
grouped_data=input_rdd.groupByKey()
average=grouped_data.mapValues(lambda x:round(sum(x)/len(x),2)).sortBy(lambda x:x[0])
for x in average.collect():
    print(x)
