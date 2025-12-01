from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

# 1. Create SparkSession
spark = SparkSession.builder.appName("PySparkHDFSExample").getOrCreate()

# 2. Read text file from HDFS
hdfs_path = "hdfs://localhost:9000/user/test/input/data.txt"
rdd = spark.sparkContext.textFile(hdfs_path)

# 3. Count total words
word_count = rdd.count()

print("=== HDFS File Word Count ===")
print(f"Total lines in HDFS file: {word_count}")

# 4. Convert all words to uppercase
upper_rdd = rdd.map(lambda x: x.upper())
result = upper_rdd.collect()

print("=== Uppercase Words ===")
for word in result:
    print(word)

# 5. Stop Spark session
spark.stop()
