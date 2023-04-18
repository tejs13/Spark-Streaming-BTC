import pandas as pd
from pyspark.sql import SparkSession

import matplotlib.pyplot as plt
from pyspark.sql.functions import col


# Create a SparkSession
# spark = SparkSession.builder.appName("CO-RELATION CHECK").getOrCreate()

# Load the Excel file into a Pandas DataFrame
excel_data = pd.read_excel("BTC_Transaction_DATASET.xlsx")

# # Convert the Pandas DataFrame to a Spark DataFrame
# spark_data = spark.createDataFrame(excel_data)
#
# # Show the first 10 rows of the Spark DataFrame
# spark_data.show(10)


plt.scatter(excel_data['size'], excel_data['trans_fees_2'])
plt.xlabel('size')
plt.ylabel('trans_fees')
# Set the x and y limits
plt.xlim(100, 1000)
plt.ylim(0, 100)

plt.title('Scatter Plot')
plt.savefig("co-relation.png")
