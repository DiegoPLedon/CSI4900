from py2neo import Graph


graph = Graph("bolt://neo4j:1Test123@ec2-3-212-83-143.compute-1.amazonaws.com:7687/db/data/")

"Function Deletes all nodes in the database when called"
def delete_all_nodes(graph):
    graph.run("MATCH (n) DETACH DELETE n")
    print("All data has been deleted")

"Returns the nodes that are in a loop"
def loop_nodes(graph):
    query = """
    MATCH p=(n)-[*]->(n)
    RETURN nodes(p)
    """
    data = graph.run(query)
    for d in data:
        print(d)


if __name__ == '__main__':
    while True:
        print("These are the possible actions for the graph: ")
        print("1: Retrieve the nodes that are in a loop")
        print("2: Delete all nodes and relationships from graph")
        input = input("Enter the number for the action to be done on the graph: ")
        if input == "1":
            print("Retrieving all looped nodes")
            loop_nodes(graph)
            break
        if input == "1":
            print("Deleting everything from graph")
            delete_all_nodes(graph)
            break

