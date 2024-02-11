from pyspark import SparkContext
# Create a SparkContext
sc = SparkContext("local[*]","broadCast")

# Create a broadcast variable
broadcast_var = sc.broadcast([10, 20])

# Access the value of the broadcast variable
print(broadcast_var.value)

# Use the value of the broadcast variable
rdd = sc.parallelize([1, 2, 3])
rdd = rdd.map(lambda x: x + broadcast_var.value[0])

# Print the results of the RDD
print(rdd.collect())