from pyspark import SparkContext
#from pyspark.accumulators import AccumulatorParam

sc=SparkContext("local[*]","accumulator")
input=sc.textFile("D:/BIGDATA/TrendyTech/week-10-spark/dataset/empty_lines.txt")
v_accum=sc.accumulator(0)
for x in input.collect():
    if(x==""):
        v_accum.add(1)
print(v_accum.value)