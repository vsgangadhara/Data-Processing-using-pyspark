import pyspark

def loadBoringWords():
  """
  This function reads a list of boring words from a file and returns a set of strings.
  """
  boringWords = set()
  with open("D:/BIGDATA/TrendyTech/week-10-spark/dataset/boringwords.txt", "r") as f:
    for line in f:
      boringWords.add(line.strip())

  return boringWords

def main():
  sc = pyspark.SparkContext("local[*]", "WordCount")
  name_set = sc.broadcast(loadBoringWords())

  initial_rdd = sc.textFile("D:/BIGDATA/TrendyTech/week-10-spark/dataset/bigdatacampaigndata.csv")
  mappedInput = initial_rdd.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))
  words = mappedInput.flatMapValues(lambda x: x.split(" "))
  finalMapped = words.map(lambda x: (x[1].lower(), x[0]))
  filter_rdd = finalMapped.filter(lambda x: x[0] not in name_set.value)
#total = filter_rdd.reduceByKey(lambda x, y: x + y)
 # sorted = total.sortBy(lambda x: x[1], False)
  for x in filter_rdd.collect():
    print(x)

if __name__ == "__main__":
  main()