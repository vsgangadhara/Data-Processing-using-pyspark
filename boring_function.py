
def loadBoringWords():
  """
  This function reads a list of boring words from a file and returns a set of strings.
  """
  boringWords = set()
  with open("D:/BIGDATA/TrendyTech/week-10-spark/dataset/boringwords.txt", "r") as f:
    for line in f:
      boringWords.add(line.strip())

  return boringWords
