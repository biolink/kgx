import logging, yaml
import itertools
from .source import Source

from typing import Union

from neo4j.v1 import GraphDatabase
from neo4j.v1.types import Node, Record

neo4j_log = logging.getLogger("neo4j.bolt")
neo4j_log.setLevel(logging.WARNING)

class NeoSource(Source):
    """
    TODO: use bolt

    We expect a Translator canonical style http://bit.ly/tr-kg-standard
    E.g. predicates are names with underscores, not IDs.

    Does not load from config file if uri and username are provided.

    TODO: also support mapping from Monarch neo4j
    """

    def __init__(self, sink, uri=None, username=None, password=None):
        super(NeoSource, self).__init__(sink)
        if uri is username is None:
            with open("config.yml", 'r') as ymlfile:
                cfg = yaml.load(ymlfile)
                uri = "bolt://{}:{}".format(cfg['neo4j']['host'], cfg['neo4j']['port'])
                username = cfg['neo4j']['username']
                password = cfg['neo4j']['password']
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def load(self, start=0, end=None):
        """
        Read a neo4j database and stream it to a sink
        """
        for page in self.get_pages(self.get_nodes, start=start, end=end):
            self.load_nodes(page)
        for page in self.get_pages(self.get_edges, start=start, end=end):
            self.load_edges(page)

    def load_nodes(self, node_records):
        """
        Load nodes from neo4j records
        """
        for node in node_records:
            self.load_node(node)

    def load_edges(self, edge_records):
        """
        Load edges from neo4j records
        """
        for edge in edge_records:
            self.load_edge(edge)

    def load_node(self, node_record:Union[Node, Record]):
        """
        Load node from a neo4j record
        """
        node = None
        if isinstance(node_record, Node):
            node = node_record
        elif isinstance(node_record, Record):
            node = node_record[0]

        attributes = {}
        for key, value in node.items():
            attributes[key] = value
        if 'labels' not in attributes:
            attributes['labels'] = list(node.labels)

        node_id = node['id'] if 'id' in node else node.id
        self.sink.add_node(node_id, attributes)

    def load_edge(self, edge_record):
        """
        Load an edge from a neo4j record
        """
        edge_subject = edge_record[0]
        edge_predicate = edge_record[1]
        edge_object = edge_record[2]

        subject_id = edge_subject['id'] if 'id' in edge_subject else edge_subject.id
        object_id = edge_object['id'] if 'id' in edge_object else edge_object.id

        attributes = {}
        for key, value in edge_predicate.items():
            attributes[key] = value
        if 'id' not in attributes:
            attributes['id'] = edge_predicate.id
        if 'type' not in attributes:
            attributes['type'] = edge_predicate.type

        self.sink.add_edge(subject_id, object_id, attributes)

    def get_pages(self, query, start=0, end=None, page_size=50000):
        """
        Gets (end - start) many pages of size page_size.
        """
        with self.driver.session() as session:
            for i in itertools.count(0):
                skip = start + (page_size * i)
                limit = page_size if end == None or skip + page_size <= end else end - skip
                if limit <= 0:
                    return

                records = session.read_transaction(query, skip=skip, limit=limit)
                if records.peek() != None:
                    yield records
                else:
                    return

    def get_nodes(self, tx, skip, limit):
        """
        Get a page of nodes from the database
        """
        labels = None
        if 'subject_category' in self.filter:
            labels = self.filter['subject_category']
        if 'object_category' in self.filter:
            labels += ':' + self.filter['object_category']

        properties = {}
        for key in self.filter:
            if key not in ['subject_category', 'object_category', 'edge_label']:
                properties[key] = self.filter[key]

        query = None
        if labels:
            query="""
            MATCH (n:{labels} {{ {properties} }}) RETURN n SKIP {skip} LIMIT {limit};
            """.format(labels=labels, properties=self.parse_properties(properties), skip=skip, limit=limit).strip()
        else:
            query="""
            MATCH (n {{ {properties} }}) RETURN n SKIP {skip} LIMIT {limit};
            """.format(properties=self.parse_properties(properties), skip=skip, limit=limit).strip()

        query = query.replace("{  }", "")

        logging.debug(query)
        return tx.run(query)

    def get_edges(self, tx, skip, limit):
        """
        Get a page of edges from the database
        """
        params = {}
        params['subject_category'] = self.filter['subject_category'] if 'subject_category' in self.filter else None
        params['object_category'] = self.filter['object_category'] if 'object_category' in self.filter else None
        params['edge_label'] = self.filter['edge_label'] if 'edge_label' in self.filter else None

        properties = {}
        for key in self.filter:
            if key not in ['subject_category', 'object_category', 'edge_label']:
                properties[key] = self.filter[key]

        query = None
        if params['subject_category'] is not None and params['object_category'] is not None and params['edge_label'] is not None:
            query = """
            MATCH (s:{subject_category})-[p:{edge_label} {{ {edge_properties} }}]->(o:{object_category})
            RETURN s,p,o
            SKIP {skip} LIMIT {limit};
            """.format(skip=skip, limit=limit, edge_properties=self.parse_properties(properties), **params).strip()

        elif params['subject_category'] is None and params['object_category'] is not None and params['edge_label'] is not None:
            query = """
            MATCH (s)-[p:{edge_label} {{ {edge_properties} }}]->(o:{object_category})
            RETURN s,p,o
            SKIP {skip} LIMIT {limit};
            """.format(skip=skip, limit=limit, edge_properties=self.parse_properties(properties), **params).strip()

        elif params['subject_category'] is None and params['object_category'] is None and params['edge_label'] is not None:
            query = """
            MATCH (s)-[p:{edge_label} {{ {edge_properties} }}]->(o)
            RETURN s,p,o
            SKIP {skip} LIMIT {limit};
            """.format(skip=skip, limit=limit, edge_properties=self.parse_properties(properties), **params).strip()

        else:
            query = """
            MATCH (s)-[p {{ {edge_properties} }}]->(o)
            RETURN s,p,o
            SKIP {skip} LIMIT {limit};
            """.format(skip=skip, limit=limit, edge_properties=self.parse_properties(properties), **params).strip()

        query = query.replace("{  }", "")

        logging.debug(query)
        return tx.run(query)

    @staticmethod
    def parse_properties(properties, delim = '|'):
        propertyList = []
        for key in properties:
            if key in ['subject', 'predicate', 'object']:
                continue

            values = properties[key]
            if type(values) == type(""):
                pair = "{}: \"{}\"".format(key, str(values))
            else:
                pair = "{}: {}".format(key, str(values))
            propertyList.append(pair)
        return ','.join(propertyList)
