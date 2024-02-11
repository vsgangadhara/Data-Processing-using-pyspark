from pyspark import SparkContext
if __name__=="__main__":
    sc=SparkContext("local[*]","average friends")
    input=sc.textFile("D:/BIGDATA/TrendyTech/week9-spark1/dataset/friendsdata.csv")
    input_split=input.map(lambda x:(x.split(",")[2],int(x.split(",")[3])))
    map_input=input_split.mapValues(lambda x:(x,1))
    reduce_map=map_input.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
    avg_map=reduce_map.mapValues(lambda x:x[0]/x[1])
    result=avg_map.collect()
    for i in result:
        print(i)