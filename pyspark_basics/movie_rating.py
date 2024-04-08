from pyspark import SparkContext

if __name__=="__main__":
    sc=SparkContext("local[*]","movie rating")
    input=sc.textFile("D:/BIGDATA/TrendyTech/week9-spark1/dataset/moviedata.data")
    map_input=input.map(lambda x:x.split("\t")[2])
    map_tup=map_input.map(lambda x:(x,1))
    reduce_input=map_tup.reduceByKey(lambda x,y:x+y)
    sorted_result=reduce_input.sortBy(lambda x:x[1],False)
    result=sorted_result.collect()
    for i in result:
        print(i)