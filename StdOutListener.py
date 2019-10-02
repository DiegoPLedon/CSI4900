from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

#need dev privileges here for tokens.
access_token = "(get your own)"
access_token_secret =  "(get your own)"
consumer_key =  "(get your own)"
consumer_secret =  "(get your own)"

class StdOutListener(StreamListener):
#Override methods
    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")
