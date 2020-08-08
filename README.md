# Real Time Twitter data streaming ETL Pipeline and Sentiment Analyser
A real-time streaming ETL pipeline for streaming Twitter data using Apache Kafka, Apache Spark and Delta Lake. Sentiment analysis is performed using Spark ML library on the data, before being persisted into the database.

## Requirements
* [Tweepy](http://docs.tweepy.org/en/latest/index.html)
* [Apache Kafka](https://kafka.apache.org/downloads) version 2.5.0
* [Apache Spark](https://spark.apache.org/downloads.html) version 3.0.0
* [kafka-python](https://pypi.org/project/kafka-python) version 2.0.1
* [pySpark](https://pypi.org/project/pyspark/) 3.0.0
* [Delta Lake](https://docs.delta.io/latest/quick-start.html) package

## Usage
After providing Twitter API credentials in twitter_credentials.py and creating kafka topics and updating the topic in tweet_stream_producer.py and tweet_stream_consumer.py, run the twitter stream using 
```
python tweet_stream_producer.py
```
and consumer as
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0 tweet_stream_consumer.py
```
The packages include dependencies for kafka and delta lake.
## Architecture

![Architecture](/images/Architecture.png)

## Extraction

### Twitter API
Tweets are streamed using Tweepy, a python based library for accessing the Twitter API.
http://docs.tweepy.org/en/latest/index.html

### Apache Kafka
Apache Kafka is an open-source stream-processing software platform developed by the Apache Software Foundation.

#### Creating Kafka topic and configuring the producer
```
/bin/kafka-topics.sh --create \
    --zookeeper <hostname>:<port> \
    --topic <topic-name> \
    --partitions <number-of-partitions> \
    --replication-factor <number-of-replicating-servers>
```
Kafka organises records as topics. The tweet stream we get from the API is published to a topic by a producer.
Kafka producer is initialized using kafka-python, which is the python client for Kafka.

## Transformation
The ingested data is read with the help of Apache Spark structured streaming.
Apache Spark structured streaming handles streaming data and provides data in the form of dataframes, as opposed to Spark Streaming
which provides data as RDDs (Resilient Distributed Datasets).

#### Pre-processing data and making predictions
The Tweet data received is then pre-processed and passed into a machine learning pipeline we are about to create.

## Building an ML Pipeline for Sentiment Analysis
Stanford's [Sentiment140](http://help.sentiment140.com/for-students) dataset is used to train a Logistic Regression model to predict
sentiment of a text.
Spark ML library is used to perform TF-IDF (Term Frequence-Inverse Document Frequency), followed by classification using Logistic Regression on the dataset.
##### Building the pipeline
A machine learning pipeline is created using Spark ML library.

![ML Pipeline](/images/MLPipeline.png)

The data is initially tokenized and filtered of urls and punctuations using regular expressions before being passed into the pipeline, where
the stop words are removed. It is then vectorized and term frequency is calculated with the help of CountVectorizer. The next step is to calculate the Inverse Document Frequency. <br/>The TF-IDF product is then passed on to a classifier, Logistic Regression in this case, to make predictions. This particular implementation had an accuracy of 0.8323919410115472, as calculated by a binary classification evaluator. <br/>The pipeline as well as trained model is saved for our use with the streaming data.

## Loading
The transformed data can be written using the DataStreamWriter API into many built in [output sinks](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks).
For debugging purposes, the console output sink can be used to display the transformed dataframe.
For the purpose of this project, the data is being written into a Databricks [Delta Lake](https://docs.databricks.com/delta/delta-streaming.html) storage layer.<br> The data can further be analyzed or visualized using tools like Tableau or PowerBI to gather insights.
