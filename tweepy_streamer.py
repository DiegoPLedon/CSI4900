from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

import twitter_credentials

"""
#Twitter Stream
class TwitterStreamer():
    Class for streaming and processing live tweets
    

    def __init__(self):
        pass

    def stream_tweets(self, fetched_tweets_file, hash_tag_list):


        # This line filter Twitter Streams to capture data by the keywords:
        stream.filter(track=hash_tag_list)
"""

#Stream Listener
class StdOutListener(StreamListener):
    """
    Prints out data with stdOut
    Sends tweet data to Kafka server
    Appends tweet data ti JSON File
    """

    def __init__(self, fetched_tweets_file):
        self.fetched_tweets_file = fetched_tweets_file

    def on_data(self, data):
        producer.send_messages("Ottawa", data.encode('utf-8'))
        try:
            print(data)
            with open(self.fetched_tweets_file, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    #hash_tag_list = ["ottawa", "Canada"]
    fetched_tweets_file = "tweets.json"

    input_string = input("Enter the tweet keywords to search by:\n")
    hash_tag_list = input_string.replace(" ", " ").split(",")
    print(hash_tag_list)


    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)
    listener = StdOutListener(fetched_tweets_file)

    auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_KEY_SECRET)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    stream = Stream(auth, listener)
    stream.filter(track=hash_tag_list)

    #twitter_streamer = TwitterStreamer()
    #twitter_streamer.stream_tweets(fetched_tweets_file, hash_tag_list)
