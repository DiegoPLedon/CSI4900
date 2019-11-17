
import json
from py2neo import Graph, Node, Relationship

# B
from kafka import KafkaConsumer

graph = Graph("bolt://neo4j:1234@localhost:7687/db/data/")

graph.run("CREATE CONSTRAINT ON (u:User) ASSERT u.username IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE")
graph.run("CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE")


def add_user(user):
    temp = Node("User", id=user["id"], name=user["name"], username=user["screen_name"].lower())
    graph.create(temp)


def relate(user1, type, user2):
    user_node_1 = graph.nodes.match("User", username=user1.lower()).first()
    user_node_2 = graph.nodes.match("User", username=user2['screen_name'].lower()).first()
    if (not user_node_2):
        temp = Node("User", id=user2["id"], name=user2["name"], username=user2["screen_name"].lower())
    else:
        temp = user_node_2
    print("Connection", user1, user2['screen_name'])
    graph.create(Relationship(user_node_1, type, temp))


consumer = KafkaConsumer('Ottawa')
for msg in consumer:
    msg_deserialized = json.loads(msg.value)
    m = graph.nodes.match("User", username=msg_deserialized['user']['screen_name'].lower()).first()
    if (not m):
        add_user(msg_deserialized['user'])
    if ('quoted_status' in msg_deserialized and 'extended_tweet' in msg_deserialized['quoted_status']):
        for mention in msg_deserialized['quoted_status']['extended_tweet']['entities']['user_mentions']:
            relate(msg_deserialized['user']['screen_name'], "MENTIONS", mention)