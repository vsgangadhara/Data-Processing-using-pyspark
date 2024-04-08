from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Student Marks Average")
sc = SparkContext(conf=conf)
student_marks = [
    ("John", 85),
    ("Jane", 92),
    ("John", 78),
    ("Jane", 88),
    ("Alice", 90),
    ("Bob", 75),
    ("Bob", 80),
]

marks_rdd = sc.parallelize(student_marks)
grouped_rdd = marks_rdd.groupByKey()
average_marks_rdd = grouped_rdd.mapValues(lambda marks: sum(marks) / len(marks))
result = average_marks_rdd.collectAsMap()
for student, average_mark in result.items():
    print(f"{student}: Average Marks = {average_mark:.2f}")
sc.stop()