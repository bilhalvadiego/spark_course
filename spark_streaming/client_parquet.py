import os
from dotenv import load_dotenv

load_dotenv()

import findspark
findspark.init()

from pyspark.sql import SparkSession


import shutil

for item in ['./check','./parquet']:
    try:
        shutil.rmtree(item)
    except OSError as err:
        print(f'Aviso: {err.strerror}')

spark = SparkSession.builder.appName('SparkStreaming').getOrCreate()
tweets = (
    spark.readStream
    .format('socket')
    .option('host', 'localhost')
    .option('port', 9009)
    .load()
)

query = (
    tweets.writeStream
    .outputMode('append')
    .option('encoding','utf-8')
    .format('parquet')
    .option('path','./parquet')
    .option('checkpointLocation','./check')
    .start()
)
query.awaitTermination()