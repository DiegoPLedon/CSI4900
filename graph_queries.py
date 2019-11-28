from py2neo import Graph

graph = Graph("bolt://neo4j:1Test123@ec2-3-212-83-143.compute-1.amazonaws.com:7687/db/data/")

"Function Deletes all nodes in the database when called"
def delete_all_nodes(graph):
    graph.delete_all()

def most_popular_node(graph):
    graph.run("")

if __name__ == '__main__':
    delete_all_nodes(graph)
