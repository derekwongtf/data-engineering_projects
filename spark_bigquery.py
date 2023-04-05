#!/usr/bin/env python
# coding: utf-8
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-east1-514344192808-w5oek8kv')

df_202203 = spark.read.parquet('gs://dtc_data_lake_de-project-381616/pq/china/hk/hong-kong/2022/03/*')
df_202206 = spark.read.parquet('gs://dtc_data_lake_de-project-381616/pq/china/hk/hong-kong/2022/06/*')
df_202209 = spark.read.parquet('gs://dtc_data_lake_de-project-381616/pq/china/hk/hong-kong/2022/09/*')
df_202212 = spark.read.parquet('gs://dtc_data_lake_de-project-381616/pq/china/hk/hong-kong/2022/12/*')

df_202203 = df_202203.withColumn('source_date', F.lit('202203'))
df_202206 = df_202206.withColumn('source_date', F.lit('202206'))
df_202209 = df_202209.withColumn('source_date', F.lit('202209'))
df_202212 = df_202212.withColumn('source_date', F.lit('202212'))

df = df_202203.unionByName(df_202206, allowMissingColumns=True)
df = df.unionByName(df_202209, allowMissingColumns=True)
df = df.unionByName(df_202212, allowMissingColumns=True)

df = df.withColumn('price_in_numbers', F.regexp_replace('price', '[$,]', '').cast('double'))


df.createOrReplaceTempView('hk_data')

df_result = spark.sql("""
SELECT
source_date,
room_type, 
round(avg(availability_30),2) avg_availability_30,
round(avg(accommodates),2) avg_accommodates,
round(avg(price_in_numbers),2) avg_price
FROM
    hk_data
WHERE
    availability_30>0
group by source_date,room_type
order by 1,2
""")


df_result.write.format('bigquery') \
    .option('table', 'data_all.report-2022') \
    .save()

