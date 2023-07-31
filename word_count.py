from pyspark import SparkContext
if __name__=="__main__":
    sc=SparkContext("local[*]","word Count")
    sc.setLogLevel("ERROR")
    input=sc.textFile("D:/BIGDATA/TrendyTech/week9-spark1/sparkinput.txt")
    word_split=input.flatMap(lambda x:x.split(" "))
    word_map  =word_split.map(lambda x:(x.upper(),1))
    word_reduce=word_map.reduceByKey(lambda x,y:x+y)
    word_sorted=word_reduce.sortBy(lambda x:x[1],False)
    result=word_sorted.collect()
    for i in result:
        print(i)
