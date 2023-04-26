import traceback

from pyspark.sql import SparkSession
import findspark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator

findspark.init('D:\Spark\spark-3.2.3-bin-hadoop3.2')



def linear_regression_test():
    spark = SparkSession.builder \
        .config("spark.jars", "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/spark-sql-kafka-0-10_2.12-3.2.3.jar" + "," +
                "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/kafka-clients-3.2.3.jar" + "," +
                "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/commons-pool2-2.8.0.jar" + "," +
                "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/spark-token-provider-kafka-0-10_2.12-3.2.3.jar") \
        .appName("BTC_Transactions_Fees_Prediction") \
        .getOrCreate()


    # Finally, we can use the trained model to make predictions on new BTC transactions
    new_transactions = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("BTC_1_TEST.csv")

    # we need to assemble the features into a vector using VectorAssembler:
    assembler = VectorAssembler(inputCols=["size", "trans_fees_2"], outputCol="features")
    new_transactions = assembler.transform(new_transactions)

    # Load the saved model
    saved_trained_model = LinearRegressionModel.load(
        "E:/CSUF/Spring 2023/531 Adv Database/Projects/Spark-Streaming-BTC/TRAINED_MODELS/regression_model")

    predictions = saved_trained_model.transform(new_transactions)

    predictions = predictions.toPandas()
    predictions.to_excel('BTC_Transaction_PREDICTION_test.xlsx', sheet_name='Sheet1', index=True)


if __name__ == '__main__':

    try:
        linear_regression_test()

    except Exception as e:
        traceback.print_exc()
        exit("Error in Spark App")
