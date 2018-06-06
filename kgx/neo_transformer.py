import pandas as pd
import networkx as nx
import logging, yaml
import itertools
from .transformer import Transformer

from typing import Union

from neo4j.v1 import GraphDatabase
from neo4j.v1.types import Node, Record

neo4j_log = logging.getLogger("neo4j.bolt")
neo4j_log.setLevel(logging.WARNING)

class NeoTransformer(Transformer):
    """
    TODO: use bolt

    We expect a Translator canonical style http://bit.ly/tr-kg-standard
    E.g. predicates are names with underscores, not IDs.

    Does not load from config file if uri and username are provided.

    TODO: also support mapping from Monarch neo4j
    """

    remap_node_properties = []
    remap_edge_properties = []

    def __init__(self, graph=None, uri=None, username=None, password=None):
        super(NeoTransformer, self).__init__(graph)

        if uri is username is None:
            with open("config.yml", 'r') as ymlfile:
                cfg = yaml.load(ymlfile)
                uri = "bolt://{}:{}".format(cfg['neo4j']['host'], cfg['neo4j']['port'])
                username = cfg['neo4j']['username']
                password = cfg['neo4j']['password']

        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def get_pages(self, query, start=0, end=None, page_size=1000):
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

                if records.peek() is not None:
                    yield records
                else:
                    return

    def load(self, start=0, end=None):
        """
        Read a neo4j database and create a nx graph
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

        if len(self.remap_node_properties):
            for pair in self.remap_node_properties:
                if pair[1] in attributes:
                    logging.debug("remap {} with {}".format(pair[0], pair[1]))
                    attributes[pair[0]] = attributes[pair[1]]
                else:
                    logging.warning("remap {} with {} failed; property {} missing from node attributes".format(pair[0], pair[1], pair[1]))

        node_id = attributes['id'] if 'id' in attributes else node.id
        self.graph.add_node(node_id, attr_dict=attributes)

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

        if 'subject' not in attributes:
            attributes['subject'] = edge_subject['id']
        if 'object' not in attributes:
            attributes['object'] = edge_object['id']

        if 'id' not in attributes:
            attributes['id'] = edge_predicate.id

        if 'type' not in attributes:
            attributes['type'] = edge_predicate.type

        if 'predicate' not in attributes:
            attributes['predicate'] = attributes['type']

        if subject_id not in self.graph.nodes():
            self.load_node(edge_subject)

        if object_id not in self.graph.nodes():
            self.load_node(edge_object)

        if len(self.remap_edge_properties):
            for pair in self.remap_edge_properties:
                if pair[1] in attributes:
                    logging.debug("remap {} with {}".format(pair[0], pair[1]))
                    attributes[pair[0]] = attributes[pair[1]]
                else:
                    logging.warning("remap {} with {} failed; property {} missing from edge attributes".format(pair[0], pair[1], pair[1]))

        self.graph.add_edge(
            subject_id,
            object_id,
            attr_dict=attributes
        )

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

    def save_node(self, tx, obj):
        """
        Load a node into neo4j
        """

        if 'id' not in obj:
            raise KeyError("node does not have 'id' property")
        if 'name' not in obj:
            logging.warning("node does not have 'name' property")

        if 'category' not in obj:
            logging.warning("node does not have 'category' property. Using 'named_thing' as default")
            label = 'named_thing'
        else:
            label = obj.pop('category')

        properties = ', '.join('n.{0}=${0}'.format(k) for k in obj.keys())
        query = "MERGE (n:{label} {{id: $id}}) ON CREATE SET {properties}".format(label=label, properties=properties)
        tx.run(query, **obj)

    def save_node_unwind(self, nodes_by_category, property_names):
        """
        Save all nodes into neo4j using the UNWIND cypher clause
        """

        for category in nodes_by_category.keys():
            self.populate_missing_properties(nodes_by_category[category], property_names)
            query = self.generate_unwind_node_query(category, property_names)
            with self.driver.session() as session:
                session.run(query, nodes=nodes_by_category[category])

    def save_edge_unwind(self, edges_by_relationship_type, property_names):
        """
        Save all edges into neo4j using the UNWIND cypher clause
        """

        for predicate in edges_by_relationship_type:
            self.populate_missing_properties(edges_by_relationship_type[predicate], property_names)
            query = self.generate_unwind_edge_query(predicate, property_names)
            edges = edges_by_relationship_type[predicate]
            for i in range(0, len(edges), 1000):
                end = i + 1000
                subset = edges[i:end]
                logging.info("edges subset: {}-{}".format(i, end))
                with self.driver.session() as session:
                    session.run(query, relationship=predicate, edges=subset)

    def generate_unwind_node_query(self, label, property_names):
        """
        Generate UNWIND cypher clause for a given label and property names (optional)
        """
        # TODO: Load this list from a config file? https://github.com/NCATS-Tangerine/kgx/issues/65
        ignore_list = []

        properties_dict = {p : p for p in property_names if p not in ignore_list}

        properties = ', '.join('n.{0}=node.{0}'.format(k) for k in properties_dict.keys() if k != 'id')

        query = """\
        UNWIND $nodes AS node\
        MERGE (n:{label} {{id: node.id}})\
        ON CREATE SET {properties}\
        """.format(label=label, properties=properties)

        logging.debug(query)

        return query


    def generate_unwind_edge_query(self, relationship, property_names):
        """
        Generate UNWIND cypher clause for a given relationship
        """
        ignore_list = ['subject', 'predicate', 'object']
        properties_dict = {p : "edge.{}".format(p) for p in property_names if p not in ignore_list}

        properties = ', '.join('r.{0}=edge.{0}'.format(k) for k in properties_dict.keys())

        query="""\
        UNWIND $edges AS edge\
        MATCH (s {{id: edge.subject}}), (o {{id: edge.object}})\
        MERGE (s)-[r:{edge_label}]->(o)\
        ON CREATE SET {properties}\
        """.format(properties=properties, edge_label=relationship)

        logging.debug(query)

        return query

    def save_edge(self, tx, obj):
        """
        Load an edge into neo4j
        """
        label = obj.pop('predicate')
        subject_id = obj.pop('subject')
        object_id = obj.pop('object')

        properties = ', '.join('r.{0}=${0}'.format(k) for k in obj.keys())

        q="""\
        MATCH (s {{id: $subject_id}}), (o {{id: $object_id}})\
        MERGE (s)-[r:{label}]->(o)\
        ON CREATE SET {properties}\
        """.format(properties=properties, label=label)

        tx.run(q, subject_id=subject_id, object_id=object_id, **obj)

    def save_from_csv(self, nodes_filename, edges_filename):
        """
        Load from a CSV to neo4j
        """
        nodes_df = pd.read_csv(nodes_filename)
        edges_df = pd.read_csv(edges_filename)

        with self.driver.session() as session:
            for index, row in nodes_df.iterrows():
                session.write_transaction(self.save_node, row.to_dict())
            for index, row in edges_df.iterrows():
                session.write_transaction(self.save_edge, row.to_dict())
        self.neo4j_report()

    def save_with_unwind(self):
        """
        Load from a nx graph to neo4j using the UNWIND cypher clause
        """

        nodes_by_category = {}
        node_properties = []
        for n in self.graph.nodes():
            node = self.graph.node[n]
            if 'id' not in node:
                continue
            if 'category' not in node:
                node['category'] = 'named_thing'
            if type(node['category']) is type([]):
                category = ':'.join(node['category'])
                if category not in nodes_by_category:
                    nodes_by_category[category] = [node]
                else:
                    nodes_by_category[category].append(node)
            else:
                if node['category'] not in nodes_by_category:
                    nodes_by_category[node['category']] = [node]
                else:
                    nodes_by_category[node['category']].append(node)

            node_properties += [x for x in node if x not in node_properties]

        edges_by_relationship_type = {}
        edge_properties = []
        for n, nbrs in self.graph.adjacency_iter():
            for nbr, eattr in nbrs.items():
                for entry, adjitem in eattr.items():
                    if adjitem['predicate'] not in edges_by_relationship_type:
                        edges_by_relationship_type[adjitem['predicate']] = [adjitem]
                    else:
                        edges_by_relationship_type[adjitem['predicate']].append(adjitem)
                    edge_properties += [x for x in adjitem.keys() if x not in edge_properties]

        with self.driver.session() as session:
            session.write_transaction(self.create_constraints, nodes_by_category.keys())

        self.save_node_unwind(nodes_by_category, node_properties)
        self.save_edge_unwind(edges_by_relationship_type, edge_properties)

    def save(self):
        """
        Load from a nx graph to neo4j
        """

        labels = {'named_thing'}
        for n in self.graph.nodes():
            node = self.graph.node[n]
            if 'category' in node:
                if isinstance(node['category'], list):
                    labels.update(node['category'])
                else:
                    labels.add(node['category'])

        with self.driver.session() as session:
            session.write_transaction(self.create_constraints, labels)
            for node_id in self.graph.nodes():
                node_attributes = self.graph.node[node_id]
                if 'id' not in node_attributes:
                    node_attributes['id'] = node_id
                session.write_transaction(self.save_node, node_attributes)
            for n, nbrs in self.graph.adjacency_iter():
                for nbr, eattr in nbrs.items():
                    for entry, adjitem in eattr.items():
                        session.write_transaction(self.save_edge, adjitem)
        self.neo4j_report()

    def report(self):
        print("Total number of nodes: {}".format(len(self.graph.nodes())))
        print("Total number of edges: {}".format(len(self.graph.edges())))

    def neo4j_report(self):
        """
        Give a summary on the number of nodes and edges in neo4j database
        """
        with self.driver.session() as session:
            for r in session.run("MATCH (n) RETURN COUNT(*)"):
                logging.info("Number of Nodes: {}".format(r.values()[0]))
            for r in session.run("MATCH (s)-->(o) RETURN COUNT(*)"):
                logging.info("Number of Edges: {}".format(r.values()[0]))

    def create_constraints(self, tx, labels):
        """
        Create a unique constraint on node 'id' for all labels
        """

        query = "CREATE CONSTRAINT ON (n:{}) ASSERT n.id IS UNIQUE"
        for label in labels:
            if ':' in label:
                sub_labels = label.split(':')
                for sublabel in sub_labels:
                    print("CREATING CONSTRAINT for multiple labels: {}".format(sublabel))
                    tx.run(query.format(sublabel))
            else:
                tx.run(query.format(label))

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

    @staticmethod
    def populate_missing_properties(objs, properties):
        for obj in objs:
            missing_properties = set(properties) - set(obj.keys())
            for property in missing_properties:
                obj[property] = ''

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
