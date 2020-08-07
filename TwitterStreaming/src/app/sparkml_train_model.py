'''
Reads 'Sentiment140' dataset, trains and saves the pipeline 
using SparkML
'''
from pyspark import SparkContext

from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf
import re

from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.feature import StopWordsRemover, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pathlib import Path

sc = SparkContext()
sqlcontext = SQLContext(sc)

SRC_DIR = Path(__file__).resolve().parent

path = "path-to-sentiment140dataset" # path to the dataset

'''
About Sentiment140 dataset
From http://help.sentiment140.com/for-students
Data file format has 6 fields:
0 - the polarity of the tweet (0 = negative, 2 = neutral, 4 = positive)
1 - the id of the tweet (2087)
2 - the date of the tweet (Sat May 16 23:58:44 UTC 2009)
3 - the query (lyx). If there is no query, then this value is NO_QUERY.
4 - the user that tweeted (robotickilldozr)
5 - the text of the tweet (Lyx is cool)
'''

df = sqlcontext \
    .read \
    .format('com.databricks.spark.csv') \
    .options(header=False) \
    .load(path) \
    .selectExpr("_c0 as sentiment", "_c5 as message")

################# Tokenize the data
pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', \
        x.lower().strip()).split(), ArrayType(StringType())
    )
df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()


################# Split the dataframe into training and testing
train, test = df.randomSplit([0.8,0.2],seed = 100)

################# Create an ML Pipeline
# Peforms TF-IDF calculation and Logistic Regression
remover = StopWordsRemover(inputCol="cleaned_data", outputCol="words")
vector_tf = CountVectorizer(inputCol="words", outputCol="tf")
idf = IDF(inputCol="tf", outputCol="features", minDocFreq=3)
label_indexer = StringIndexer(inputCol = "sentiment", outputCol = "label")
logistic_regression = LogisticRegression(maxIter=100)

pipeline = Pipeline(stages=[remover, vector_tf, idf, label_indexer, logistic_regression])

################# Fit the pipeline to the training dataframe
trained_model = pipeline.fit(train)

'''
The labels are labelled with positive (4) as 0.0 
negative (0) as 1.0
'''
################# Predicting the test dataframe and calculating accuracy
prediction_df = trained_model.transform(test)

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
accuracy = evaluator.evaluate(prediction_df)
print(accuracy)# output accuracy=0.8323919410115472

################# Save the pipeline model
model_path = SRC_DIR.joinpath('models')
trained_model.write().overwrite() \
    .save(model_path)