# -*- coding: utf-8 -*-
from pyspark.sql.session import SparkSession
import shutil
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from glob import glob
import sys
from os import getenv, listdir

if __name__ == '__main__':
    print('Pismo Technical Data Engineering Test Application ')

    print('Creating Spark Application')
    spark = SparkSession.builder.master(getenv('SPARK_MASTER')).getOrCreate()

    if len(glob('/data/*.json')) == 0:
        print('Warning! Add sample data to process on `/data/` directory')
        sys.exit()

    print('Reading /data/*.json')
    source = spark.read.option('multiline', True).json('/data/*.json')

    print('Adding identifiers column')
    domain_data = source\
    .withColumn('unique_event', F.concat(col('domain'), col('event_type')))\
    .withColumn('domain_id', col('data.id'))

    print('Dropping Duplicates')

    pismo_window = Window.partitionBy(['unique_event', 'domain_id']).orderBy(col('timestamp').desc())
    duplicated =  domain_data\
    .withColumn('row_number', F.row_number().over(pismo_window))

    print('Total Duplicated registries', duplicated.where(col('row_number') > 1).count())
    pismo_data = duplicated\
    .where(col('row_number') == 1)\
    .drop('row_number')

    print('Time partitioning')
    date_column = 'timestamp'

    final = pismo_data\
    .withColumn('year', F.year(date_column))\
    .withColumn('month', F.month(date_column))\
    .withColumn('day', F.dayofmonth(date_column))

    print('Saving to /output')
    final.write.mode('overwrite').partitionBy(['domain', 'year', 'month', 'day']).parquet('/output/pismo-data/')

    # Moves the output to /data/output
