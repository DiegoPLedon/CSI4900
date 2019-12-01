import sys


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer
from kafka import KafkaClient
import twitter_credentials
import itertools as it



class StdOutListener(StreamListener):
    """
    Prints out data with stdOut
    Sends tweet data to Kafka server
    Appends tweet data ti JSON File
    """

    def __init__(self, fetched_tweets_file):
        self.fetched_tweets_file = fetched_tweets_file

    def on_data(self, data):
        producer.send_messages("finaltest", data.encode('utf-8'))
        try:
            print(data)
            return True
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # hash_tag_list = ["ottawa", "Canada"]
    fetched_tweets_file = "tweets.json"

    key_word = input("Enter a domain (general topic) to search by:\n")
    others = input("Enter subcategories (e.g vulnerabilities) to search for [No spaces, separate by comma]: \n")
    hash_tag_list = others.strip().split(",")
    key_words_list = [None]*len(hash_tag_list)
    for i in range(len(hash_tag_list)):
        key_words_list[i] = key_word + " " + hash_tag_list[i]
    print(key_words_list)

    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)
    listener = StdOutListener(fetched_tweets_file)

    auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_KEY_SECRET)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    stream = Stream(auth, listener)
    stream.filter(track=key_words_list)

    # twitter_streamer = TwitterStreamer()
    # twitter_streamer.stream_tweets(fetched_tweets_file, hash_tag_list)
