# -*- coding: utf-8 -*-
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    source = spark.read.json('./data/*.json')

    print(source.count())
    print(source.head())

    print('add_columns')
    birth_column = 'birthdate'

    df = source\
            .withColumn('birth_year', F.year(birth_column))\
            .withColumn('birth_month', F.month(birth_column))\
    .withColumn('birth_day', F.dayofmonth(birth_column))

    print('Dropping Duplicates')

    print('Save')

    df.write.partitionBy(['birth_year', 'birth_month', 'birth_day']).parquet('output-data')

