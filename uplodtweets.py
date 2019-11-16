import os
import sys
import time
import requests
from py2neo import Graph, Node, Relationship

graph = Graph()

graph.run("CREATE CONSTRAINT ON (u:User) ASSERT u.username IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE")

def upload_tweets(tweets):
    for t in tweets:
        u = t["user"]
        e = t["entities"]

        tweet = Node("Tweet", id=t["id"])
        graph.merge(tweet)
        tweet["text"] = t["text"]
        tweet.push()

        user = Node("user", username=u["screen_name"])
        graph.merge(user)

        graph.merge(Relationship(user, "POSTS", tweet))
#Nodes
        for h in e.get("hashtags", []):
            hashtag = Node("Hashtag", name=h["text"].lower())
            graph.merge(hashtag)
            graph.merge(Relationship(hashtag, "TAGS", tweet))

        for m in e.get('user_mentions', []):
            mention = Node("User", username=m["screen_name"])
            graph.merge(mention)
            graph.merge(Relationship(tweet, "MENTIONS", mention))

        for r in e.get('in_reply_to_user_id', []):
            mention = Node("User", username=r["in_reply_to_user_id_str"])
            graph.merge(mention)
            graph.merge(Relationship(tweet, "REPLY", mention))

        reply = t.get("in_reply_to_status_id")

        if reply:
            reply_tweet = Node("Tweet", id=reply)
            graph.merge(reply_tweet)
            graph.merge(Relationship(tweet, "REPLY_TO", reply_tweet))

        ret = t.get("retweeted_status", {}).get("id")

        if ret:
            retweet = Node("Tweet", id=ret)
            graph.merge(retweet)
            graph.merge(Relationship(tweet, "RETWEETS", retweet))