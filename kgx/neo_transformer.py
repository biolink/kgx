import pandas as pd
import networkx as nx
import logging, yaml
from .transformer import Transformer

from neo4j.v1 import GraphDatabase

class NeoTransformer(Transformer):
    """
    TODO: use bolt

    We expect a Translator canonical style http://bit.ly/tr-kg-standard
    E.g. predicates are names with underscores, not IDs.

    TODO: also support mapping from Monarch neo4j
    """

    def __init__(self, t=None):
        super(NeoTransformer, self).__init__(t)
        with open("config.yml", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)

        uri = "bolt://{}:{}".format(cfg['neo4j']['host'], cfg['neo4j']['port'])
        self.driver = GraphDatabase.driver(uri, auth=(cfg['neo4j']['username'], cfg['neo4j']['password']))

    def load(self):
        """
        Read a neo4j database and create a nx graph
        """
        pass

    def load_node(self, tx, obj):
        """
        Load a node into neo4j
        """
        tx.run("CREATE (n {params})", params=obj)

    def load_edge(self, tx, obj):
        """
        Load an edge into neo4j
        """

        s = obj['subject']
        p = obj['predicate']
        o = obj['object']

        del obj['subject']
        del obj['predicate']
        del obj['object']

        queryString = self.build_cypher_string(s, p, o, obj)
        logging.debug(queryString)
        tx.run(queryString)

    def build_cypher_string(self, s, p, o, params=None):
        """
        Build cypher query from given subject, predicate, object and relationship properties
        """
        if ':' in p:
            # relationship name cannot have ':'
            p = p.replace(':', '_')
        if params:
            queryString = "MATCH (s {id: '" + s + "'}) MATCH (o {id: '" + o + "'}) MERGE (s)-[:" + p + " {"
            for key in params:
                queryString += " " + key + ": '{}'".format(NeoTransformer.parse_value(params[key], '|')) + ","
            queryString = queryString[:-1]
            queryString += "}]->(o)"
        else:
            queryString = "MATCH (s {id: '" + s + "'}) MATCH (o {id: '" + o + "'}) MERGE (s)-[:" + p + "]->(o)"
        return queryString

    def save_from_csv(self, nodes_filename, edges_filename):
        """
        Load from a CSV to neo4j
        """
        nodes_df = pd.read_csv(nodes_filename)
        edges_df = pd.read_csv(edges_filename)

        with self.driver.session() as session:
            for index, row in nodes_df.iterrows():
                session.write_transaction(self.load_node, row.to_dict())
            for index, row in edges_df.iterrows():
                session.write_transaction(self.load_edge, row.to_dict())
        self.neo4j_report()

    def save(self):
        """
        Load from a nx graph to neo4j
        """

        with self.driver.session() as session:
            for n in self.graph.nodes():
                # TODO: node attributes
                session.write_transaction(self.load_node, {'id': n})
            for n, nbrs in self.graph.adjacency_iter():
                for nbr, eattr in nbrs.items():
                    for entry, adjitem in eattr.items():
                        session.write_transaction(self.load_edge, adjitem)
        self.neo4j_report()

    def neo4j_report(self):
        """
        Give a summary on the number of nodes and edges in neo4j database
        """
        with self.driver.session() as session:
            for r in session.run("MATCH (n) RETURN COUNT(*)"):
                logging.info("Number of Nodes: {}".format(r.values()[0]))
            for r in session.run("MATCH (s)-->(o) RETURN COUNT(*)"):
                logging.info("Number of Edges: {}".format(r.values()[0]))

    @staticmethod
    def parse_value(value, delimiter):
        """
        Parse multi-valued properties separated by a delimiter
        """
        if delimiter in value:
            valueString = "["
            for v in value.split(delimiter):
                valueString += v + ","
            valueString = valueString[:-1]
            valueString += "]"
        else:
            valueString = value
        return valueString

class MonarchNeoTransformer(NeoTransformer):
    """
    TODO: do we need a subclass, or just make parent configurable?

    In contrast to a generic import/export, the Monarch neo4j graph
    uses reification (same as Richard's semmeddb implementation in neo4j).
    This transform should de-reify.

    Also:

     - rdf:label to name
     - neo4j label to category
    """
    
