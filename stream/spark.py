import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from pyspark.sql.functions import from_json, window, col, avg, to_timestamp, date_format, expr, abs
from pyspark.sql.types import StructField, StructType, DoubleType, IntegerType, StringType, LongType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, last, max_by
import pyspark.sql.functions as F

# 1. setup spark session
spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

topic_streaming = "stream-data"

# 2. Data stream from kakfa
# data_stream = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", stream) \
#     .option("startingOffsets", "earliest") \
#     .load()

data_stream = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9091) \
    .load()
data_json_schema = StructType([
    StructField("stockName", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])
data_stream = data_stream.selectExpr("cast(value as string) as value")
data_stream = data_stream.withColumn("value", from_json(
    data_stream["value"], data_json_schema)).select("value.*")
data_stream = data_stream.withColumn("timestamp", to_timestamp(
    data_stream["timestamp"], "HH:mm:ss")).withWatermark("timestamp", "1 second")



# 3. Config stream from socket
rule_stream = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9090) \
    .load()

rule_json_schema = StructType([
    StructField("stockNameRule", StringType()),
    StructField("priceRule", DoubleType()),
    StructField("timestampRule", TimestampType(), True),
])
rule_stream = rule_stream.selectExpr("cast(value as string) as value")
rule_stream = rule_stream \
    .select(from_json("value", rule_json_schema).alias("data")) \
    .select("data.*")

# {"stockName": "a", "price": 21} {"stockNameRule": "a", "priceRule": 10}

# 5. Handle join stream
def processEachDataRow(row):
    print(row)
def processEachRuleRow(row):
    print(row)

queryData = data_stream  \
    .writeStream \
    .foreach(processEachDataRow) \
    .start()

queryRule = rule_stream \
    .writeStream \
    .foreach(processEachRuleRow) \
    .start()

spark.streams.awaitAnyTermination()

# use built-in smtp server debugger: python -m smtpd -c DebuggingServer -n localhost:1025
# send email to alert user
def send_email(subject, body, to_email, smtp_server='localhost', smtp_port=1025):
    # Set up the MIME
    msg = MIMEMultipart()
    msg['From'] = 'SparkSteaming@gmail.com'  # Replace with your email address
    msg['To'] = to_email
    msg['Subject'] = subject

    # Attach the message body
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    body_with_timestamp = f"{body}\n\n At: {timestamp}"
    msg.attach(MIMEText(body_with_timestamp, 'plain'))

    try:
        # Connect to the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)

        # Send the email
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        print("Email sent successfully!")

    except Exception as e:
        print(f"Error: Unable to send email. {e}")

    finally:
        # Close the connection
        server.quit()