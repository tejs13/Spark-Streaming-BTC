from kafka import KafkaConsumer

import socket


# consumer = KafkaConsumer('bitcoin', group_id='active-group')
#
#
# for msg in consumer:
#     print(msg)
client_socket = socket.socket()  # instantiate
client_socket.connect(('localhost', 6666))

while True:
    msg = client_socket.recv(2048).decode()
    print(msg, "=================")


from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, DoubleType

# Define the schema for the transaction data
schema = StructType([
    StructField("op", StringType()),
    StructField("x", StructType([
        StructField("lock_time", IntegerType()),
        StructField("ver", IntegerType()),
        StructField("size", IntegerType()),
        StructField("inputs", ArrayType(StructType([
            StructField("sequence", IntegerType()),
            StructField("prev_out", StructType([
                StructField("spent", BooleanType()),
                StructField("tx_index", IntegerType()),
                StructField("type", IntegerType()),
                StructField("addr", StringType()),
                StructField("value", IntegerType()),
                StructField("n", IntegerType()),
                StructField("script", StringType())
            ])),
            StructField("script", StringType())
        ]))),
        StructField("time", IntegerType()),
        StructField("tx_index", IntegerType()),
        StructField("vin_sz", IntegerType()),
        StructField("hash", StringType()),
        StructField("vout_sz", IntegerType()),
        StructField("relayed_by", StringType()),
        StructField("out", ArrayType(StructType([
            StructField("spent", BooleanType()),
            StructField("tx_index", IntegerType()),
            StructField("type", IntegerType()),
            StructField("addr", StringType()),
            StructField("value", IntegerType()),
            StructField("n", IntegerType()),
            StructField("script", StringType())
        ])))
    ]))
])

# Load the transaction JSON data into a Spark DataFrame
df = spark.read.json("path/to/transaction.json", schema=schema)

# Flatten the inputs and out columns using explode
df = df.select(
    "x.lock_time",
    "x.ver",
    "x.size",
    "x.time",
    "x.tx_index",
    "x.vin_sz",
    "x.hash",
    "x.vout_sz",
    "x.relayed_by",
    explode(col("x.inputs")).alias("input"),
    explode(col("x.out")).alias("output")
)

# Flatten the prev_out column inside the inputs column
df = df.select(
    "*",
    "input.prev_out.spent",
    "input.prev_out.tx_index",
    "input.prev_out.type",
    "input.prev_out.addr",
    "input.prev_out.value",
    "input.prev_out.n",
    "input.prev_out.script"
).drop("input.prev_out")

# Flatten the output column
df = df.select(
    "*",
    "output.spent",
    "output.tx_index",
    "output.type",
    "output.addr",
    "output.value",
    "output.n",
    "output.script"
).drop("output")

# Show the flattened DataFrame
df.show()





from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

# Define the schema for the JSON data
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

# Load the JSON data into a DataFrame
df = spark.read.json("path/to/json", schema=schema)

# Select the required columns and explode the inputs and out arrays
inputs_df = df.selectExpr("x.hash as hash", "explode(x.inputs) as inputs")
out_df = df.selectExpr("x.hash as hash", "explode(x.out) as out")

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

# Show the resulting flattened DataFrame
joined_df.show()





