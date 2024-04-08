from pyspark import SparkContext
sc=SparkContext("local[*]","reduce")
a=range(1,10)
input=sc.parallelize(a)
reduce_input=input.reduce(lambda x,y:x+y)
print(reduce_input)

