%spark.pyspark

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["date", "price"]

data = []
start_date = datetime(2023, 1, 1)
for i in range(1000):
    price = round(random.uniform(10, 100), 2)
    data.append((start_date.strftime("%Y-%m-%d"), price))
    start_date += timedelta(days=1)

# Create DataFrame with inferred schema
df = spark.createDataFrame(data, schema=columns)

df.show()

--------------------------------------------------------------------------------

%spark.pyspark
%matplotlib inline
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd

def simple_moving_average(df, period=3):
    window_spec = Window.orderBy("date").rowsBetween(1 - period, 0)
    df = df.withColumn("sma", F.round(F.avg("price").over(window_spec), 2))
    df_sma = df.select("date", "price", "sma")
    return df_sma

period_sma = int(z.input("Enter SMA period: ", 3))  
df_sma = simple_moving_average(df, period_sma)

pandas_df = df_sma.toPandas()
# df_sma.createOrReplaceTempView('sma') to switch to sql

# # Convert Spark DataFrame to Pandas DataFrame for plotting
pandas_df = df_sma.toPandas()
pandas_df['date'] = pd.to_datetime(pandas_df['date'])

# Plotting price and sma on the same plot using Matplotlib
plt.figure(figsize=(22, 8))
plt.plot(pandas_df['date'], pandas_df['price'], label='Price')
plt.plot(pandas_df['date'], pandas_df['sma'], label='SMA')
plt.xlabel('Date')
plt.ylabel('Value')
plt.title('Price and Simple Moving Average (SMA)')
plt.legend()
plt.xticks(rotation=45)

plt.show()
