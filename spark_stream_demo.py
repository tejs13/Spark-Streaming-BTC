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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, ArrayType

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




    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

        # .option("maxOffsetsPerTrigger", "100") \


    # schema = StructType([
    #     StructField("op",StringType(),True),
    #     StructField("x",StringType(),True),
    #     StructField("lock_time",IntegerType(),True),
    #     # StructField("ISO_3166_2", StringType(), True),
    #     # StructField("DATE", DateType(), True),
    #     # StructField("GROCERY_AND_PHARMACY_CHANGE_PERC", StringType(), True), #using StringType
    #     # StructField("PARKS_CHANGE_PERC", StringType(), True), #using StringType
    #     # StructField("RESIDENTIAL_CHANGE_PERC", IntegerType(), True),
    #     # StructField("RETAIL_AND_RECREATION_CHANGE_PERC", IntegerType(), True),
    #     # StructField("TRANSIT_STATIONS_CHANGE_PERC", IntegerType(), True),
    #     # StructField("WORKPLACES_CHANGE_PERC", IntegerType(), True),
    #     # StructField("LAST_UPDATE_DATE", StringType(), True), #using StringType
    #     # StructField("LAST_REPORTED_FLAG", BooleanType(), True),
    #     # StructField("SUB_REGION_2", StringType(), True),
    #   ])

    schema = StructType([
        StructField("op", StringType(), True),
        StructField("x", StructType([
            StructField("lock_time", IntegerType(), True),
            StructField("ver", IntegerType(), True),
            StructField("size", IntegerType(), True),
            StructField("inputs", ArrayType(StructType([
                StructField("sequence", IntegerType(), True),
                StructField("prev_out", StructType([
                    StructField("spent", BooleanType(), True),
                    StructField("tx_index", IntegerType(), True),
                    StructField("type", IntegerType(), True),
                    StructField("addr", StringType(), True),
                    StructField("value", IntegerType(), True),
                    StructField("n", IntegerType(), True),
                    StructField("script", StringType(), True)
                ]), True),
                StructField("script", StringType(), True)
            ]), True)),
            StructField("time", IntegerType(), True),
            StructField("tx_index", IntegerType(), True),
            StructField("vin_sz", IntegerType(), True),
            StructField("hash", StringType(), True),
            StructField("vout_sz", IntegerType(), True),
            StructField("relayed_by", StringType(), True),
            StructField("out", ArrayType(StructType([
                StructField("spent", BooleanType(), True),
                StructField("tx_index", IntegerType(), True),
                StructField("type", IntegerType(), True),
                StructField("addr", StringType(), True),
                StructField("value", IntegerType(), True),
                StructField("n", IntegerType(), True),
                StructField("script", StringType(), True)
            ]), True))
        ]), True)
    ])

    # Convert the binary column to a string column and parse the JSON
    parsed_df = df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    print(type(parsed_df), "+=================")

    # Select the required columns and explode the inputs and out arrays
    inputs_df = parsed_df.selectExpr("x.hash as hash", "explode(x.inputs) as inputs")
    out_df = parsed_df.selectExpr("x.hash as hash", "explode(x.out) as out")

    # Flatten the inputs and out arrays
    inputs_flattened_df = inputs_df.selectExpr("hash", "inputs.sequence as sequence", "inputs.prev_out.spent as spent",
                                               "inputs.prev_out.tx_index as tx_index", "inputs.prev_out.type as type",
                                               "inputs.prev_out.addr as addr", "inputs.prev_out.value as value",
                                               "inputs.prev_out.n as n", "inputs.prev_out.script as script",
                                               "inputs.script as input_script")

    out_flattened_df = out_df.selectExpr("hash", "out.spent as spent", "out.tx_index as tx_index", "out.type as type",
                                         "out.addr as addr", "out.value as value", "out.n as n", "out.script as script")

    # Join the inputs and out DataFrames on the common column "hash"
    joined_df = inputs_flattened_df.join(out_flattened_df, "hash")


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
        # print(df.show())
        joined_df.describe().toPandas().to_excel('BTC_Transaction.xlsx', sheet_name='Sheet1', index=True)

        cnt += 1

    joined_df.writeStream \
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
        server_socket.listen(1)
        print("Waiting for the CLient COnnection !!!!!!!!!!!!!!1")
        conn, address = server_socket.accept()
        print(" CLient connected succesfully, iniate spark")
        conn.send("tejas".encode())  # send data to the client



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