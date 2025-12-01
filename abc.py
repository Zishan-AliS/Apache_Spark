from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ReadNumbers").getOrCreate()
hdfs_path = "hdfs://localhost:9000/25_nov/input_25_nov.txt"
rdd = spark.sparkContext.textFile(hdfs_path)
print("=== Numbers in file ===")
print(rdd.collect())
spark.stop()
