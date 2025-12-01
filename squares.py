from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Squares").getOrCreate()

rdd = spark.sparkContext.textFile("numbers.txt")

numbers = rdd.map(lambda x: int(x))
squares = numbers.map(lambda x: (x, x*x))

print("=== Number & Square ===")
print(squares.collect())

spark.stop()
