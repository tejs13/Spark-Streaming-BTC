import os
import traceback
import pandas

import findspark
import websocket
from websocket import create_connection

findspark.init('D:\Spark\spark-3.2.3-bin-hadoop3.2')


from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, ArrayType, \
    LongType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "bitcoin"

import socket




cnt = 0

def spark_start_job(conn=None):

    spark = SparkSession.builder\
        .config("spark.jars", "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/spark-sql-kafka-0-10_2.12-3.2.3.jar" + "," +
                "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/kafka-clients-3.2.3.jar" + "," +
                "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/commons-pool2-2.8.0.jar" + "," +
                "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/spark-token-provider-kafka-0-10_2.12-3.2.3.jar")\
        .appName("bitcoin-transactions")\
        .getOrCreate()\
        # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")

    # Reduce logging
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")
    # spark.sparkContext.setLogLevel("INFO")


    # remove NOT required columns from here, just comment out
    schema = StructType([
        StructField("op", StringType(), True),
        StructField("x", StructType([
            StructField("lock_time", IntegerType(), True),
                StructField("ver", IntegerType(), True),
                StructField("size", IntegerType(), True),
                StructField("inputs", ArrayType(StructType([
                    StructField("sequence", LongType(), True),
                    StructField("prev_out", StructType([
                        StructField("spent", BooleanType(), True),
                        StructField("tx_index", LongType(), True),
                        StructField("type", LongType(), True),
                        StructField("addr", StringType(), True),
                        StructField("value", LongType(), True),
                        StructField("n", LongType(), True),
                        StructField("script", StringType(), True)
                    ]), True),
                    StructField("script", StringType(), True),
                ]), True), True),

            StructField("time", LongType(), True),
            StructField("tx_index", LongType(), True),
            StructField("vin_sz", LongType(), True),
            StructField("hash", StringType(), True),
            StructField("vout_sz", LongType(), True),
            StructField("relayed_by", StringType(), True),
            StructField("out", ArrayType(StructType([
                        StructField("spent", BooleanType(), True),
                        StructField("tx_index", LongType(), True),
                        StructField("type", LongType(), True),
                        StructField("addr", StringType(), True),
                        StructField("value", LongType(), True),
                        StructField("n", LongType(), True),
                        StructField("script", StringType(), True)
                    ]), True))
        ]), True)
    ])

    # temp read from json
    # .option("multiLine", 'true') \
    df = spark.readStream \
        .option("maxFilesPerTrigger", 1) \
        .format('json')\
        .schema(schema) \
        .json("BTC_TRANSACTIONS")



    def process_each_batch(df, batch_id):
        global cnt
        # Collect the output of the stream into a Pandas dataframe
        # output_df = df.toPandas()
        # Print the output to the console
        # server_socket.send(df)

        # if listener connected then, sink the output
        if conn:
            conn.send(b'tejas zalak')

        print(df, "Finally", "========================", cnt, type(df))


        print(df.show())

        df.toPandas().to_excel('BTC_Transaction.xlsx', sheet_name='Sheet1', index=True)
        # df.describe().toPandas().to_excel('BTC_Transaction.xlsx', sheet_name='Sheet1', index=True)

        cnt += 1


    df.writeStream \
        .format("console") \
        .outputMode("append") \
        .foreachBatch(process_each_batch)\
        .start() \
        .awaitTermination()



if __name__ == '__main__':


    try:
        server_socket = socket.socket()
        server_socket.bind(('localhost', 6666))
        # configure how many client the server can listen simultaneously
        # server_socket.listen(1)
        # print("Waiting for the CLient COnnection !!!!!!!!!!!!!!1")
        # conn, address = server_socket.accept()
        # print(" CLient connected succesfully, iniate spark")
        # conn.send("tejas".encode())  # send data to the client
        #


        # print("socket sent")
        # conn.close()

        # without socket
        spark_start_job()
        # with Sink Socket
        # spark_start_job(conn)

    except BrokenPipeError:
        server_socket.close()
        exit("Pipe Broken, Exiting...")
    except KeyboardInterrupt:
        server_socket.close()
        exit("Keyboard Interrupt, Exiting..")
    except Exception as e:
        traceback.print_exc()
        server_socket.close()
        exit("Error in Spark App")































# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
#
# spark = SparkSession.builder.master("local[2]") \
#   .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3") \
#   .getOrCreate()
#
#
# df = spark.readStream\
#   .format("kafka")\
#   .option("subscribe", 'bitcoin')\
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)\
#   .option("kafka.sasl.mechanism", "PLAIN")\
#   .option("kafka.security.protocol", "SASL_SSL")\
#    .option("kafka.request.timeout.ms", "60000")\
#   .option("kafka.session.timeout.ms", "60000")\
#   .option("failOnDataLoss", "true")\
#   .option("startingOffsets", "earliest") \
#   .option("partition", 1) \
#   .option("kafka.group.id", "grp1") \
#   .load()
#
# ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#
# ds.writeStream \
#   .trigger(processingTime='5 seconds') \
#   .outputMode("update") \
#   .format("console") \
#   .option("truncate", "false") \
#   .start()
#
# spark.streams.awaitAnyTermination()