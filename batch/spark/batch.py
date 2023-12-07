from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window
import matplotlib.pyplot as plt
import pyspark.pandas.plot
import os

spark = SparkSession \
    .builder \
    .appName("SparkBatchProcess") \
    .config("spark.cassandra.connection.host", "172.18.0.4") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()
    
# Specify keyspace and table
keyspace = "stock_data"
table = "stock_prices"

# Read data from Cassandra into a DataFrame
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=table, keyspace=keyspace) \
    .load()

# df.printSchema()

def simple_moving_average(df, period=3):
    window_spec = Window.orderBy("date").rowsBetween(1 - period, 0)
    df = df.withColumn('sma', F.round(F.avg("close").over(window_spec), 2))
    pandas_df = df.select("date", "close", "sma").toPandas()
    print("SMA: ")
    print(pandas_df)
    
    # plt.figure(figsize=(10, 6))
    # plt.plot(pandas_df['date'], pandas_df['close'], label='Price', marker='o', linestyle='-', color='blue')
    # plt.plot(pandas_df['date'], pandas_df['sma'], label='3-days Moving Avg', marker='o', linestyle='-', color='orange')
    # plt.xlabel('Date')
    # plt.ylabel('Value')
    # plt.title('Price and 3-days Moving Average')
    # plt.legend()
    # plt.xticks(rotation=45)
    # plt.tight_layout()
    # plt.show()


def cumulative_moving_average(df):
    window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn('cma', F.round(F.avg("close").over(window_spec), 2))
    pandas_df = df.select("date", "close", "cma").toPandas()
    print("CMA: ")
    print(pandas_df)

def exponential_moving_average(df, period = 3):
    pass
    
simple_moving_average(df=df, period=3)
cumulative_moving_average(df=df)

spark.stop()