import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# create spark session
spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#kafka topics
topic_data = "data"
topic_config = "config"
topic_result = "result"

# data stream
data_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092") \
    .option("subscribe", topic_data) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false")\
    .load() \
    .selectExpr("cast(value as string) as value")
data_schema = StructType([
    StructField("date", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("adjClose", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("name", StringType(), True)
])
data_stream = data_stream.withColumn("value", from_json(
    data_stream["value"], data_schema))
data_stream = data_stream.select("value.*").withWatermark("date", "1 day")

#config stream
config_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092") \
    .option("subscribe", topic_config) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false")\
    .load() \
    .selectExpr("cast(value as string) as value")
config_schema = StructType([
    StructField("name", StringType(), True),
    StructField("priceTypeConfig", StringType(), True),
    StructField("conditionConfig", StringType(), True),
    StructField("priceConfig", DoubleType(), True),
    StructField("timestampConfig", TimestampType(), True),
])
config_stream = config_stream.withColumn("value", from_json(
    config_stream["value"], config_schema))
config_stream = config_stream.select("value.*").withWatermark("timestampConfig", "1 second")

# join stream-stream
join_stream = data_stream.join(
    config_stream,
    (data_stream["name"] == config_stream["name"]) & (data_stream["close"] >= config_stream["priceConfig"])
)

# Process batches
def send_email(body):
    sender_email = 'bau4813@gmail.com'  # Email người gửi
    receiver_email = 'baupro06@gmail.com'  # Email người nhận
    password = 'yefv uqfm mllo vegw'  # Password email người gửi

    subject = 'Thông báo về dữ liệu'  # Tiêu đề email

    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = receiver_email
    message['Subject'] = subject

    # Tạo nội dung email với thông tin được chuyển đến
    body = f"Date: {body['date']}\nClose: {body['close']}\nPrice Config: {body['priceConfig']}\nTimestamp Config: {body['timestampConfig']}"
    message.attach(MIMEText(body, 'plain'))

    try:
        # Kết nối đến máy chủ SMTP của email provider
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)

        # Gửi email
        server.sendmail(sender_email, receiver_email, message.as_string())
        print("Email đã được gửi thành công!")

    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")

    finally:
        # Đóng kết nối
        server.quit()
        
# Process each batch using foreachPartition
def process_batch(df, epoch_id):
    # Order df to get the last row
    max_date_row = df.orderBy(["date", "timestampConfig"], ascending=[False, False]).limit(1)
    
    if max_date_row.count() > 0:
        # Get the last match row
        max_date = max_date_row.collect()[0]
        
        # Send the row with highest date and timestampConfig in the batch through email
        send_email(max_date)
        
# handle join stream
query = join_stream.writeStream \
    .foreachBatch(process_batch)\
    .start()
query.awaitTermination()   

        
