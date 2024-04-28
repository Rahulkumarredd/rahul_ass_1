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

average_temp_df = splitDF.select(month(col('ObservationDate')).alias('month'),year(col('ObservationDate')).alias('year'),col('AirTemperature')).groupBy('month','year').agg(avg('AirTemperature')).orderBy('year','month')
average_temp_df.show(15)

spark.stop()

