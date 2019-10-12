import sys
from json import loads
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaConsumer
from tweepy_streamer import StdOutListener
if sys.version_info[0] < 3:
    import got
else:
    import GetOldTweets3 as got

def main():
    def printTweet(t):
        print("Username: %s" % t.username)
        print("Retweets: %d" % t.retweets)
        print("Text: %s" % t.text)
        print("Mentions: %s" % t.mentions)
        print("Hashtags: %s" % t.hashtags)
        print("Date: %s\n" % t.date)

    # Example 3 - Get tweets by username and bound dates
    tweet_criteria = got.manager.TweetCriteria().setQuerySearch("bluekeep").setSince("2016-09-11").setMaxTweets(100)
    tweet = got.manager.TweetManager.getTweets(tweet_criteria)
    fetched_tweets_file = "old_tweets.json"

    consumer = KafkaConsumer (
        'oldTweets',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id = 'CSI4900',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka)
    listener = StdOutListener(fetched_tweets_file)
    for tweets in tweet:
        producer.send_messages("bluekeep", tweets.text.encode('utf-8'))
        try:
            printTweet(tweets)
            with open(fetched_tweets_file, 'a') as tf:
                tf.write(tweets)
        except BaseException as e:
            print("Error on_data %s" % str(e))



if __name__ == '__main__':
    main()