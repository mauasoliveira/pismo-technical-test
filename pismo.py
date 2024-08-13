# -*- coding: utf-8 -*-
from pyspark.sql.session import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    # .master('spark://spark-master')

    print('Reading /data/*.json')
    source = spark.read.json('/data/*.json')

    print('Adding identifiers column')
    domain_data = source\
    .withColumn('unique_event', F.concat(col('domain'), col('event_type')))\
    .withColumn('domain_id', col('data.id'))

    print('Dropping Duplicates')

    pismo_window = Window.partitionBy(['unique_event', 'domain_id']).orderBy(col('timestamp').desc())
    duplicated =  domain_data\
    .withColumn('row_number', F.row_number().over(pismo_window))

    print('Total Duplicated registries', duplicated.where(col('row_number') > 1).count())
    pismo_data = duplicated

    .where(col('row_number') == 1)\
    .drop('row_number')

    print('Time partitioning')
    date_column = 'timestamp'

    pismo_data = domain_id\
    .withColumn('year', F.year(date_column))\
    .withColumn('month', F.month(date_column))\
    .withColumn('day', F.dayofmonth(date_column))

    print('Saving to /output')
    pismo_data.write.partitionBy(['domain', 'year', 'month', 'day']).parquet('/output')

