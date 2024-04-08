from pyspark import SparkContext
if __name__=="__main__":
    sc=SparkContext("local[*]","average friends")
    input=sc.textFile("D:/BIGDATA/TrendyTech/week9-spark1/dataset/friendsdata.csv")
    input_split=input.map(lambda x:(x.split(",")[2],int(x.split(",")[3])))
    avg_grp=input_split.groupByKey()
    avg_map=avg_grp.map(lambda x:(x[0],sum(x[1])/len(x[1])))
    #avg_map=avg_grp.mapValues(lambda x:sum(x)/len(x))
    result=avg_map.collect()
    for x in result:
        print(x)