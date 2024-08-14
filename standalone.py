# -*- coding: utf-8 -*-

from pyspark.sql.session import SparkSession
from rich.pretty import Pretty
from rich.console import Console
import shutil
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from glob import glob
import sys
from os import getenv, listdir
from rich.prompt import Prompt
import data.Local_Fake_Data_Generator as fake_data

console: Console

SOURCE_DATA = './data/*.json'
OUTPUT_DATA = './output/standalone/'

def get_spark():
    session =  SparkSession.builder.getOrCreate()

    return session


def main(console = None):
    if not console: 
        console = Console()

    console.rule('Pismo Data Engineering Technical Test')

    if len(glob('./data/*.json')) == 0:
        console.print("[yellow]Warning![/yellow] There's no data to process!")

        create_data = console.input('Create fake data? (Y)')
        if lower(create_data) != 'y':
            sys.exit()

        fake_data.main()


    console.print('Creating Spark Application')
    spark = get_spark()

    console.print(f'Reading {SOURCE_DATA}')
    source = spark.read.option('multiline', True).json(SOURCE_DATA)

    console.rule('Statistics')
    console.print('[blue] TODO [/blue]')

    console.print('Adding identifiers column')
    domain_data = source\
    .withColumn('unique_event', F.concat(col('domain'), col('event_type')))\
    .withColumn('domain_id', col('data.id'))

    console.print('Dropping Duplicates')

    pismo_window = Window.partitionBy(['unique_event', 'domain_id']).orderBy(col('timestamp').desc())
    duplicated =  domain_data\
    .withColumn('row_number', F.row_number().over(pismo_window))

    console.print('Total Duplicated registries', duplicated.where(col('row_number') > 1).count())
    pismo_data = duplicated\
    .where(col('row_number') == 1)\
    .drop('row_number')

    console.print('Time partitioning')
    date_column = 'timestamp'

    final = pismo_data\
    .withColumn('year', F.year(date_column))\
    .withColumn('month', F.month(date_column))\
    .withColumn('day', F.dayofmonth(date_column))

    console.print(f'Saving to {OUTPUT_DATA}')
    final.repartition('domain', 'timestamp')\
    .write.mode('overwrite')\
    .partitionBy(['domain', 'year', 'month', 'day'])\
    .parquet(OUTPUT_DATA)

if __name__ == '__main__':
    console = Console()
    main(console)
