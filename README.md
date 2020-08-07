# Real Time Twitter data streaming ETL Pipeline
A real-time streaming ETL pipeline for streaming Twitter data using Apache Kafka, Apache Spark and Delta Lake.

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
Kafka producer is initialised using kafka-python, which is the python client for Kafka.

## Transformation
The ingested data is read with the help of Apache Spark structured streaming.
Apache Spark structured streaming handles streaming data and provides data in the form of dataframes, as opposed to Spark Streaming
which provides data as RDDs (Resilient Distributed Datasets).

#### Pre-processing data and making predictions
The Tweet data received is then pre-processed and then passed into a machine learning pipeline we are about to create.

#### Performing Sentiment Analysis
Stanford's [Sentiment140](http://help.sentiment140.com/for-students) dataset is used to train a Logistic Regression model to predict
sentiment of a text.
Spark ML library is used to perform TF-IDF (Term Frequence-Inverse Document Frequency) followed by classification using Logistic Regression on the dataset.
##### Building an ML pipeline
A machine learning pipeline is created using Spark ML library.

