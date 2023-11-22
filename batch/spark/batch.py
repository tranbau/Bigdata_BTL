from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window
import matplotlib.pyplot as plt
import pyspark.pandas.plot

# db config
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://127.0.0.1:5432/bigdata"
user = "postgres"
password = "bau20204813"
table_name = "stock"

spark = SparkSession.builder \
    .appName("Connecting to JDBC") \
    .master("local") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

    
df = spark.read \
    .format("jdbc") \
    .option("driver", driver) \
    .option("url", url) \
    .option("user", user) \
    .option("password", password) \
    .option("dbtable", "stock") \
    .load()

SMA = 3 # 3-days
window_spec = Window.orderBy("date").rowsBetween(1-SMA, 0)
df = df.withColumn('3-days_avg', F.round(F.avg("close").over(window_spec), 2)) 
df = df.filter(df['3-days_avg'].isNotNull())
    
df.show()
pandas_df = df.select("date", "close", "3-days_avg").toPandas()

# Plotting the line chart
plt.figure(figsize=(10, 6))

# Plotting the price line
plt.plot(pandas_df['date'], pandas_df['close'], label='Price', marker='o', linestyle='-', color='blue')

# Plotting the 3-day moving average line
plt.plot(pandas_df['date'], pandas_df['3-days_avg'], label='3-days Moving Avg', marker='o', linestyle='-', color='orange')

plt.xlabel('Date')
plt.ylabel('Value')
plt.title('Price and 3-days Moving Average')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()