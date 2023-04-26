import traceback

from pyspark.sql import SparkSession
import findspark
from pyspark.ml.feature import VectorAssembler, PolynomialExpansion
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


from pyspark.sql.functions import pow, col


findspark.init('D:\Spark\spark-3.2.3-bin-hadoop3.2')




def spark_train_model():

    spark = SparkSession.builder \
        .config("spark.jars", "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/spark-sql-kafka-0-10_2.12-3.2.3.jar" + "," +
                    "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/kafka-clients-3.2.3.jar" + "," +
                    "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/commons-pool2-2.8.0.jar" + "," +
                    "///D:/Spark/spark-3.2.3-bin-hadoop3.2/jars/spark-token-provider-kafka-0-10_2.12-3.2.3.jar")\
        .appName("BTC_Transactions_Fees_Prediction") \
        .getOrCreate()


    # Reduce logging
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")
    # spark.sparkContext.setLogLevel("INFO")

    btc_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("BTC_Transaction_TRANING_DATSET.csv")

    # we need to assemble the features into a vector using VectorAssembler:
    assembler = VectorAssembler(inputCols=["size"], outputCol="features")

    btc_df = assembler.transform(btc_df)
    btc_df.select("features").show()

    print(btc_df)

    # Create a polynomial expansion transformer with degree 2
    poly_expansion = PolynomialExpansion(degree=2, inputCol="features", outputCol="poly_features")

    # Fit the transformer to the data and transform it
    poly_data = poly_expansion.transform(btc_df)
    poly_data.printSchema()
    poly_data.show(10)


    #  we can split the dataset into training and testing sets:
    (trainingData, testData) = poly_data.randomSplit([0.7, 0.3], seed=123)


    #  create a Linear Regression model:
    lr = LinearRegression(featuresCol="poly_features", labelCol="trans_fees_2", maxIter=20, regParam=0.3, elasticNetParam=0.8)

    #  then train the model on the training dataset:
    lrModel = lr.fit(trainingData)
    # lrModel.save("E:/CSUF/Spring 2023/531 Adv Database/Projects/Spark-Streaming-BTC/TRAINED_MODELS/regression_model_2")



    #  we can evaluate the model on the testing dataset:
    predictions = lrModel.transform(testData)
    evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="trans_fees_2", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    # Convert RMSE to a percentage
    mean_label = testData.agg({"trans_fees_2": "avg"}).collect()[0][0]
    print(mean_label, "================== MEAN LABEL")
    rmse_percent = (rmse / mean_label) * 100
    print("RMSE PERCENT ==========", rmse_percent)


    # Finally, we can use the trained model to make predictions on new BTC transactions
    new_transactions = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("BTC_Transaction_TRANING_DATSET.csv")

    new_transactions = assembler.transform(new_transactions)
    # Create a polynomial expansion transformer with degree 2
    poly_expansion_2 = PolynomialExpansion(degree=2, inputCol="features", outputCol="poly_features")

    # Fit the transformer to the data and transform it
    poly_data_2 = poly_expansion_2.transform(new_transactions)

    predictions = lrModel.transform(poly_data_2)
    predictions.show(10)

    predictions = predictions.toPandas()
    predictions.to_excel('BTC_Transaction_PREDICTION_test.xlsx', sheet_name='Sheet1', index=True)


    # predictions.select("hash1", "prediction").show()




if __name__ == '__main__':
    try:
        spark_train_model()
    except Exception as e:
        traceback.print_exc()

        exit("Error in Spark App")







