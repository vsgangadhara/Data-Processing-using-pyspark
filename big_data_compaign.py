from pyspark import SparkContext
sc=SparkContext("local[*]","big data compaign")

input=sc.textFile("D:/BIGDATA/TrendyTech/week-10-spark/dataset/bigdatacampaigndata.csv")
mapped_input=input.map(lambda x:(x.split(",")[10],x.split(",")[0]))
word_split=mapped_input.flatMapValues(lambda x:x.split(" "))
word_swap=word_split.map(lambda x:(x[1],x[0]))
result=word_swap.collect()
for i in result:
    print(i)