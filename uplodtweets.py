import json
from py2neo import Graph, Node, Relationship

# B
from kafka import KafkaConsumer

graph = Graph("bolt://neo4j:1Test123@ec2-3-212-83-143.compute-1.amazonaws.com:7687/db/data/")

graph.run("CREATE CONSTRAINT ON (u:User) ASSERT u.username IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE")


def add_user(user):
    temp = Node("User", id=user["id"], name=user["name"], username=user["screen_name"].lower())
    graph.create(temp)


def add_hashtag(hashtag):
    temp = Node("Hashtag", name=hashtag["text"].lower())
    graph.create(temp)


def relate_users(user1, type, user2):
    user_node_1 = graph.nodes.match("User", username=user1.lower()).first()
    user_node_2 = graph.nodes.match("User", username=user2['screen_name'].lower()).first()
    if (not user_node_2):
        temp = Node("User", id=user2["id"], name=user2["name"], username=user2["screen_name"].lower())
    else:
        temp = user_node_2
    print("Connection", user1, user2['screen_name'])
    graph.create(Relationship(user_node_1, type, temp))


def relate_hashtag_to_user(user, hashtag):
    user_node = graph.nodes.match("User", username=user["screen_name"].lower()).first()
    if (not user_node):
        temp = Node("User", id=user["id"], name=user["name"], username=user["screen_name"].lower())
    print(hashtag)
    print("Hashtag", user['screen_name'])
    graph.create(Relationship(user_node, "Hashtag", hashtag))


consumer = KafkaConsumer('Ottawa')
for msg in consumer:
    msg_deserialized = json.loads(msg.value)
    print()
    try:
        m = graph.nodes.match("User", username=msg_deserialized['user']['screen_name'].lower()).first()
    except:
        print("Rate limit hit, waiting...")
        continue

    if (not m):
        add_user(msg_deserialized['user'])
    for mention in msg_deserialized['entities']['user_mentions']:
        relate_users(msg_deserialized['user']['screen_name'], "MENTIONS", mention)
    if ('quoted_status' in msg_deserialized and 'extended_tweet' in msg_deserialized['quoted_status']):
        for mention in msg_deserialized['quoted_status']['extended_tweet']['entities']['user_mentions']:
            relate_users(msg_deserialized['user']['screen_name'], "MENTIONS", mention)
    for hashtag in msg_deserialized['entities']['hashtags']:
        hashtag_node = graph.nodes.match("Hashtag", name=hashtag['text'].lower()).first()
        if (not hashtag_node):
            add_hashtag(hashtag)
        hashtag_node = graph.nodes.match("Hashtag", name=hashtag['text'].lower()).first()
        relate_hashtag_to_user(msg_deserialized['user'], hashtag_node)
