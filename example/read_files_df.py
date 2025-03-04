from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('DataFrame') \
    .master('local[*]') \
    .getOrCreate()

df = spark.read.csv()