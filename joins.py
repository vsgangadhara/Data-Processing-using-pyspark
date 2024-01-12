from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Sample data for left DataFrame
left_data = [("Alice", 1),
             ("Bob", 2),
             ("Charlie", 3)]

left_columns = ["name", "value_left"]
left_df = spark.createDataFrame(left_data, left_columns)

# Sample data for right DataFrame
right_data = [("Alice", 10),
              ("David", 20),
              ("Eve", 30)]

right_columns = ["name", "value_right"]
right_df = spark.createDataFrame(right_data, right_columns)

# Inner Join
inner_join_df = left_df.join(right_df, on="name", how="inner")

# Left Join
left_join_df = left_df.join(right_df, on="name", how="left")

# Right Join
right_join_df = left_df.join(right_df, on="name", how="right")

# Outer Join (or Full Join)
outer_join_df = left_df.join(right_df, on="name", how="outer")

# Show the results
print("Inner Join:")
inner_join_df.show()

print("Left Join:")
left_join_df.show()

print("Right Join:")
right_join_df.show()

print("Outer Join:")
outer_join_df.show()

# Stop the Spark session
spark.stop()
