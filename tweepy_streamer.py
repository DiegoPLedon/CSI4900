from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

import twitter_credentials

class StdOutListener(StreamListener):
#Override methods from StreamListener

	#takes in data streamed in from StreamListener
    def on_data(self, data):
        print (data)
        return True
	#prints out error that occurs
    def on_error(self, status):
        print (status)
		
if __name__ == "__main__":
	#Object of inherited class
	listener = StdOutListener()
	
	#kafka = KafkaClient("localhost:9092")
	#producer = SimpleProducer(kafka)
	
	#Add keys in twitter_credentials.py
	auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_KEY_SECRET)
	auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
	
	stream = Stream(auth, listener)
	
	#Filtering tweets
	stream.filter(track=['Diego'])
	