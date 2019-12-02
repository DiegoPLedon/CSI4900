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

def page_rank(graph):
    query = """
    CALL algo.pageRank.stream('User', 'MENTIONS', {iterations:20, dampingFactor:0.85})
    YIELD nodeId, score

    RETURN algo.asNode(nodeId).name AS name,score
    ORDER BY score DESC LIMIT 10
    """
    data = graph.run(query)
    for d in data:
        print(d)

def most_connected_nodes(graph):
    query = """
        MATCH (a)-[:MENTIONS]->(b)
        RETURN b, COLLECT(a) as authors
        ORDER BY SIZE(authors) DESC LIMIT 10
        """
    data = graph.run(query)
    for d in data:
        print(d)

def most_used_hashtags(graph):
    query = """
    MATCH (a)-[:Hashtag]->(b)
    RETURN b, COLLECT(a) as authors
    ORDER BY SIZE(authors) DESC LIMIT 10
    """
    data = graph.run(query)
    for d in data:
        print(d)

def choice():
    program_running = True
    while program_running:
        print("These are the possible actions for the graph: ")
        print("0: Exit from program")
        print("1: Retrieve the nodes that are in a loop")
        print("2: Delete all nodes and relationships from graph")
        print("3: Return the Page rank of the top 10 Nodes by running the Page score algorithm")
        print("4: Return the Nodes with the most connections")
        print("5: Return the most used Hashtags")
        inp = input("Enter the number for the action to be done on the graph: ")
        print("")
        if inp == "0":
            print("Exiting from program...\n")
            program_running = False
        if inp == "1":
            print("Deleting everything from graph")
            delete_all_nodes(graph)
        if inp == "2":
            print("Retrieving all looped nodes")
            loop_nodes(graph)
            print("")
        if inp == "3":
            print("Returning the Page rank of the nodes in the graph: \n")
            page_rank(graph)
            print("")
        if inp == "4":
            print("Returning the most used Hashtags: \n")
            most_connected_nodes(graph)
            print("")
        if inp == "5":
            print("Returning the most used Hashtags: \n")
            most_used_hashtags(graph)
            print("")
        else:
            print("Input not valid, please type the number of the option you want, and click enter\n")


if __name__ == '__main__':
    choice()

