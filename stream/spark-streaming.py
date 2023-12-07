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
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
topic = "data"

# 2. Data stream from kakfa
data_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

data_json_schema = StructType([
    StructField("stockName", StringType(), True),
    StructField("tradeType", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", LongType(), True),
    StructField("timestamp", TimestampType(), True),
])
data_stream = data_stream.selectExpr("cast(value as string) as value")
data_stream = data_stream.withColumn("value", from_json(
    data_stream["value"], data_json_schema)).select("value.*")
data_stream = data_stream.withColumn("timestamp", to_timestamp(
    data_stream["timestamp"], "HH:mm:ss")).withWatermark("timestamp", "1 day")


# 3. Config stream from socket
configs_stream = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9090) \
    .load()
configs_json_schema = StructType([
    StructField("stockNameRule", StringType()),
    StructField("tradeTypeRule", StringType(), True),
    StructField("priceRule", DoubleType()),
    StructField("quantityRule", LongType(), True),
    StructField("timestampRule", TimestampType(), True),
])
configs_stream = configs_stream \
    .select(from_json("value", configs_json_schema).alias("data")) \
    .select("data.*")
configs_stream = configs_stream.withWatermark("timestampRule", "1 second")

# 4. Join stream-stream
join_stream = data_stream.join(
    configs_stream,
    expr("""
         stockName = stockNameRule AND
         price >= priceRule
         """)
)

# 5. Handle join stream
query = join_stream  \
    .writeStream \
    .format("console")\
    .outputMode("append") \
    .start()

# spark.streams.awaitAnyTermination()
query.awaitTermination()

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