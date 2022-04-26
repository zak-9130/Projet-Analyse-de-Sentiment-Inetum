from json import loads
from pprint import pprint
from pyspark.sql.functions import expr
from pyspark.sql.types import *
import math
import string
import random
import re
from pyspark.sql import functions as F
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.utils import *
from textblob import TextBlob
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import expr

def polarity(text):
    return TextBlob(text).sentiment.polarity


def subjectivity(text):
    return TextBlob(text).sentiment.subjectivity

def feeling(text):
    if TextBlob(text).sentiment.polarity > 0:
        return 'Positive'
    elif TextBlob(text).sentiment.polarity == 0:
        return 'Neutre'
    else:
        return 'Negative'

def text_classification(words):
    # polarity detection
    polarity_udf = udf(polarity, StringType())
    words = words.withColumn("polarity", polarity_udf("Tweet"))
    # subjectivity detection
    subjectivity_udf = udf(subjectivity, StringType())
    words = words.withColumn("subjectivity", subjectivity_udf("Tweet"))
    #feeling detection
    feeling_udf=udf(feeling, StringType())
    words = words.withColumn("feeling", feeling_udf("Tweet"))
    return words

# Initialisation spark
findspark.init()

conf = SparkConf().setAppName("inetum")

spark = SparkSession.builder \
      .appName("inetum") \
      .master("local[1]") \
      .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")



message = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter") \
    .option("failOnDataLoss", False)\
    .load()

# schéma des tweets reçus
#schema_tweet= schema_of_json(lit('''{"id": " ", "text": " "}'''))

#kafka_df = message.select(from_json(col('value').cast('string'),schema=schema_tweet).alias('tweets')).withColumn('timestamp', lit(current_timestamp()))

#tweets_df= kafka_df.select('tweets.*','timestamp')


def preprocessing(lines):

    schema_tweet = schema_of_json(lit('''{"id": " ", "text": " "}'''))
    kafka_df = lines.select(from_json(col('value').cast('string'), schema=schema_tweet).alias('tweets')).withColumn(
        'timestamp', lit(current_timestamp()))

    tweets_df = kafka_df.select('tweets.*', 'timestamp')\
        .withColumn('Tweet', F.regexp_replace('text', "@[A-Za-z0-9_]+",""))
    tweets_df = tweets_df.na.replace('', None)
    tweets_df = tweets_df.na.drop()
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'@[A-Za-z0-9_]+', ''))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'#[A-Za-z0-9_]+', ''))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'http\S+', ''))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'^rt', ''))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'RT', ''))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'[^\w\s]', ''))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', r'\n', ''))
    tweets_df = tweets_df.withColumn('Tweet', F.regexp_replace('Tweet', '  ', ' '))
    tweets_df = text_classification(tweets_df)
    #tweets_df = tweets_df.repartition(1)


    #col_df=tweets_df.select(col('id'),col('timestamp'),col('ok1'))
    #text = tweets_df.select(col('text'))

    #words = tweets_df.select(col('text')).fillna(None)
    #words = words.na.drop()
    #words = words.withColumn('text', F.regexp_replace('text', r'@[A-Za-z0-9_]+', ''))
    #words = words.withColumn('text', F.regexp_replace('text', r'#[A-Za-z0-9_]+', ''))
    #words = words.withColumn('text', F.regexp_replace('text', r'^rt', ''))
    #words = words.withColumn('text', F.regexp_replace('text', 'RT', ''))
    #words = words.withColumn('text', F.regexp_replace('text', '[^\w\s]', ''))
    #words = words.withColumn('text', F.regexp_replace('text', '\n', ''))

    #words= words.withColumn('ID2', col_df.id)
    #words = words.withColumn('ok', lit(5))
    #words = words.join(col_df).filter(words.ok == col_df.ok1)
    #words = words.drop('ok')
    #words = words.withColumn('ID', col_df.id)
    #words = words.toDF('id','text', 'timestamp')
    #words = words.select(col("id").alias("ID"),col("timestamp").alias("Time"),col("text").alias("Twemet"))
    '''tweets_df.writeStream.format("csv")\
    .option("path", "/Users/zackinho91/Desktop/tweet.csv")\
    .option("checkpointLocation","new_tweet")\
    .start()'''
    return tweets_df


'''resultat = preprocessing(message)
res1 = res.writeStream\
    .format("console")\
    .start().awaitTermination()'''
'''
a=tweets_df.select(col('text'))
print(a)
res = a.writeStream\
    .format("console")\
    .start().awaitTermination()'''

resultat = preprocessing(message)
resultat.writeStream.outputMode('append')\
    .format('org.elasticsearch.spark.sql')\
    .option('es.nodes','localhost')\
    .option('es.port',9200).\
    option('es.spark.sql.streaming.sink.log.path','False')\
    .option('checkpointLocation','checkpoint')\
    .start('inetum').awaitTermination()

'''resultat.writeStream.outputMode('append')\
    .format("console")\
    .start('zak_twitter.com').awaitTermination()'''
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 consumer_pyspark.py
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.1.3 consumer_pyspark.py