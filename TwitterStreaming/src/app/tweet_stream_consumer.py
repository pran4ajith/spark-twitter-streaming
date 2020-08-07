'''
Initialize spark streaming service, which streams data from kafka topic.
Processes and trains data from the stream and saves it to a delta sink.
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql.functions import udf, from_json, col

from pyspark.ml import PipelineModel

import re
from datetime import datetime
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent

################# Add the topic created
kafka_topic = 'your-topic-name'

spark = SparkSession \
    .builder \
    .appName("StreamingApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

schema = StructType(
    [StructField("created_at", StringType()),
    StructField("message", StringType())]
    )

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("header", "true") \
    .load() \
    .selectExpr("CAST(timestamp AS TIMESTAMP) as timestamp", "CAST(value AS STRING) as message")

df = df \
    .withColumn("value", from_json("message", schema)) \
    .select('timestamp', 'value.*')

# Changing datetime format
date_process = udf(
    lambda x: datetime.strftime(
        datetime.strptime(x,'%a %b %d %H:%M:%S +0000 %Y'), '%Y-%m-%d %H:%M:%S'
        )
    )
df = df.withColumn("created_at", date_process(df.created_at))

################# Pre-processing the data
pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', \
        x.lower().strip()).split(), ArrayType(StringType())
    )
df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()

################# Passing into ml pipeline
model_path = SRC_DIR.joinpath('models')
pipeline_model = PipelineModel.load(model_path)

prediction  = pipeline_model.transform(df)

'''
The labels are labelled with positive (4) as 0.0 
negative (0) as 1.0
'''

prediction = prediction \
    .select(prediction.cleaned_data, prediction.created_at, \
         prediction.timestamp, prediction.message, prediction.prediction)

# print(prediction.schema)
################# Write to Delta

delta_output_path = SRC_DIR.parent.joinpath('delta/events/_checkpoints/twitter_predictions')
checkpoint = SRC_DIR.parent.joinpath('delta/events')
query = prediction \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint) \
    .start(delta_output_path)

query.awaitTermination()

################# Write to console
'''
query = prediction \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
query.awaitTermination()
'''