from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date, col, year, month, avg
import os

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('ACCESSKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('SECRETKEY'))
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled', 'true')
conf.set('spark.hadoop.fs.s3a.committer.name', 'magic')
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")



spark = SparkSession.builder.appName("rchaganti convert 60.txt part 3").config('spark.driver.host', 'spark-edge.service.consul').config(conf=conf).getOrCreate()

splitDF = spark.read.parquet('s3a://rchaganti/60.parquet')

# I have selected columns of Date and Temperature then groupBy operation on the month and year columns to group the data by month and year and then applied aggregate function of 'avg' to find Average temperature of AirTemperature and sorted using 'orderBy'

average_temp_df = splitDF.select(month(col('ObservationDate')).alias('month'),year(col('ObservationDate')).alias('year'),col('AirTemperature')).groupBy('month','year').agg(avg('AirTemperature')).orderBy('year','month')
average_temp_df.show(15)
# average_temp_df.write.format("parquet").mode("overwrite").option("header", "true").save("s3a://rchaganti/part-three.parquet")

first_year_df = average_temp_df.where(col('year') == 1961)

first_year_df.show(12)
# first_year_df.write.format("csv").mode("overwrite").option("header", "true").save("s3a://rchaganti/part-three.csv")

spark.stop()

