import sys

if sys.version_info[0] < 3:
    import got
else:
    import GetOldTweets3 as got
import json

def main():
    def printTweet(t):
        print("Username: %s" % t.username)
        print("Retweets: %d" % t.retweets)
        print("Text: %s" % t.text)
        print("Mentions: %s" % t.mentions)
        print("Hashtags: %s\n" % t.hashtags)

    # Example 3 - Get tweets by username and bound dates
    tweetCriteria = got.manager.TweetCriteria().setQuerySearch("canada").setSince("2016-09-11").setMaxTweets(10)
    tweet = got.manager.TweetManager.getTweets(tweetCriteria)
    for tweets in tweet:
        printTweet(tweets)


if __name__ == '__main__':
    main()