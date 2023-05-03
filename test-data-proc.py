import pyspark
from pyspark.sql import SparkSession
import json
import os
import traceback
import pandas
from google.cloud import pubsub_v1
import findspark
import requests
import websocket
from websocket import create_connection

# findspark.init('D:\Spark\spark-3.2.3-bin-hadoop3.2')


from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, from_unixtime, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, ArrayType, \
    LongType

#Create List
numbers = [1,2,1,2,3,4,4,6]
subscription_path_abs = "projects/1050341177353/locations/us-central1-a/subscriptions/btc-trans-subscriber"



def process_each_batch(df):
    print(df, "FUNALLY ========================")


spark = SparkSession.builder \
        .appName("bitcoin-transactions") \
        .getOrCreate() \

# Reduce logging
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setLogLevel("ERROR")



df = (
        spark.readStream.format("pubsublite")
        .option(
            "pubsublite.subscription",
            subscription_path_abs,
        )
        .load()
    )

# parsed_df = df.select(from_json(col("data").cast("string"), schema).alias("data"))
# df = parsed_df
df.writeStream \
        .format("console") \
        .outputMode("append") \
        .foreachBatch(process_each_batch)\
        .start() \
        .awaitTermination()

print("END oF THE SCRIPT")

# #SparkContext
# sc = pyspark.SparkContext()
#
# # Creating RDD using parallelize method of SparkContext
# rdd = sc.parallelize(numbers)
#
# #Returning distinct elements from RDD
# distinct_numbers = rdd.distinct().collect()
#
# #Print
# print('Distinct Numbers:', distinct_numbers)
#


