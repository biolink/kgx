# ## Script for x-ray graph validation against the biolink json schema
# ## When fully implemented will be a generic module that takes query from any neo4j knowledge graph as input


from neo4j.v1 import GraphDatabase

uri = "xxxx"
driver = GraphDatabase.driver(uri, auth=("xxxx", "xxxx"))


def get_all_relationship_types():
    query = '''
    match (n)-[r]-()
    return distinct type(r)
    '''
    with driver.session() as session:
        with session.begin_transaction() as tx:
            for record in tx.run(query):
                print(record)


