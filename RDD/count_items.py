from pyspark import SparkContext

sc=SparkContext("local[*]","practice")

data=[10,30,10,30,10,40,50,30,40,30,40,30,20,20,100,200,100]
rdd1=sc.parallelize(data).countByValue().items()

#print(rdd1)
for i in rdd1:
    print(i)