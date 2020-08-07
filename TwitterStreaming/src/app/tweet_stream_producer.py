'''
Streams tweets using tweepy API.
Initializes kafka producer and ingest the streaming tweets to the topic.
'''
import twitter_credentials as tc
from tweepy import OAuthHandler, StreamListener
from tweepy import Stream, API

from kafka import KafkaProducer

import json
from bson import json_util
from dateutil.parser import parse
import re

class KafkaConfig():
	def __init__(self):
		self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
	def get_producer(self):
		return self.producer

class TweetStreamListener(StreamListener):
	def on_data(self, data):
    	##### Kafka producer initialize
		kafka_producer = KafkaConfig().get_producer()
        ##### Add the topic created
		kafka_topic = 'your-topic-name'
		##### Filter necessary fields from the json data
		tweet = json.loads(data)
		tweet_text = ""
		print(tweet.keys())

		if all(x in tweet.keys() for x in ['lang', 'created_at']) and tweet['lang'] == 'en':
			if 'retweeted_status' in tweet.keys():
				if 'quoted_status' in tweet['retweeted_status'].keys():
					if('extended_tweet' in tweet['retweeted_status']['quoted_status'].keys()):
						tweet_text = tweet['retweeted_status']['quoted_status']['extended_tweet']['full_text']
				elif 'extended_tweet' in tweet['retweeted_status'].keys():
					tweet_text = tweet['retweeted_status']['extended_tweet']['full_text']
			elif tweet['truncated'] == 'true':
				tweet_text = tweet['extended_tweet']['full_text']

			else:
				tweet_text = tweet['text']

		if(tweet_text):
			data = {
				'created_at': tweet['created_at'],
				 'message': tweet_text.replace(',','')
				 }
			kafka_producer.send(kafka_topic, value = json.dumps(data, default=json_util.default).encode('utf-8'))

	def on_error(self, status):
		if(status == 420): 
			return False # 420 error occurs when rate limit exceeds
		print(status)

if __name__ == "__main__":
	
	listener = TweetStreamListener()

	#API Authentication
	auth = OAuthHandler(tc.consumer_key, tc.consumer_secret)
	auth.set_access_token(tc.access_token, tc.access_token_secret)
	api = API(auth)

	stream = Stream(api.auth, listener)
    ###### Add the tracks to filter the tweets
	stream.filter(track=["Covid19", "coronavirus", "covid"])