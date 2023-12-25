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

df = df.select("date", "close")
df = df.withColumnRenamed("close", "price")
df = df.withColumn("price", F.col("price").cast("float"))

def print_pandas(spark_df):
    pandas_df = spark_df.toPandas()
    print(pandas_df)
    
def simple_moving_average(df, period=3):
    window_spec = Window.orderBy("date").rowsBetween(1 - period, 0) # Tính T-1 phiên trước đó  + phiên hiện tại
    df = df.withColumn('sma', F.round(F.avg("price").over(window_spec), 2))
    df_sma = df.select("date", "price", "sma")
    
    return df_sma
    
def cumulative_moving_average(df):
    window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0) # Tính từ đầu
    df = df.withColumn('cma', F.round(F.avg("price").over(window_spec), 2))
    df_cma = df.select("date", "price", "cma")
    
    return df_cma

def ema_calculate(t, values, N):
    alpha = 2.0 / (N + 1) # Chỉ số alpha
    adjusted_weights = [pow(1 - alpha, t - index) for index in range(t + 1)] # [(1-a)^0 , (1-a)^1 , (1-a)^2 , ... , (1-a)^n]
    numerator = sum(map(lambda x, y: x * y, adjusted_weights, values[:t + 1])) # tử số , nhân 2 phần tử tương ứng 2 mảng và tính tổng các kết quả
    denomiator = sum(adjusted_weights)
    result =  numerator / denomiator
    
    return  result

def exponential_moving_average(df, period = 10):
    column = "price"
    orderByColumn = "date"
    
    ema_udf = F.udf(ema_calculate)
    window_spec = Window.orderBy(orderByColumn)
    df_with_t = df.withColumn("t", F.row_number().over(window_spec) - 1)
    df_ema = df_with_t.withColumn("ema", ema_udf(F.col("t"), F.collect_list(column).over(window_spec), F.lit(period))).drop("t")
    df_ema = df_ema.select("date", "price", "ema")
    
    return df_ema

def relative_strength_index(df, period = 14):
    # Tính độ tăng/giảm giữa 2 phiên liên tiếp
    window_spec = Window.orderBy('date')
    df_dis = df.withColumn('dis',F.col('price') - F.lag(F.col('price'), 1).over(window_spec))
    df_dis = df_dis.withColumn('up', F.when(F.col('dis') >= 0, F.col('dis')).otherwise(None))
    df_dis = df_dis.withColumn('down', F.when(F.col('dis') < 0, -F.col('dis')).otherwise(None))
    df_dis = df_dis.drop('dis')
    
    # Tính TBC tăng/giảm theo chu kì (14) và chia tỉ lệ ra rs
    df_avg_dis = df_dis.withColumn('avg_up', F.when(F.row_number().over(Window.orderBy('date')) > period,
                                              F.avg('up').over(window_spec)).otherwise(None))
    df_avg_dis = df_avg_dis.withColumn('avg_down', F.when(F.row_number().over(Window.orderBy('date')) > period,
                                              F.avg('down').over(window_spec)).otherwise(None))
    df_rs = df_avg_dis.withColumn('rs',F.round(F.col("avg_up") / F.col("avg_down"),2))
    
    # Tính rsi từ rs
    df_rsi = df_rs.withColumn('rsi', 100 - 100 / (1 + F.col('rs')))
    df_rsi = df_rsi.withColumn('rsi', F.round(F.col('rsi'), 2))
    df_rsi = df_rsi.select("date", "price", "rsi")
    
    return df_rsi

    
if __name__== 'main' :
    pass

spark.stop()