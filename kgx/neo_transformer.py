import pandas as pd

import logging
import yaml
import json

import itertools
import uuid
import click

from .transformer import Transformer
from .filter import Filter, FilterLocation, FilterType

from typing import Union, Dict, List
from collections import defaultdict
from neo4jrestclient.client import GraphDatabase as http_gdb, Node, Relationship

neo4j_log = logging.getLogger("neo4j.bolt")
neo4j_log.setLevel(logging.WARNING)


class NeoTransformer(Transformer):
    """

    We expect a Translator canonical style http://bit.ly/tr-kg-standard
    E.g. predicates are names with underscores, not IDs.

    Does not load from config file if uri and username are provided.

    TODO: also support mapping from Monarch neo4j
    """

    def __init__(self, graph=None, host=None, ports=None, username=None, password=None, **args):
        super(NeoTransformer, self).__init__(graph)

        self.http_driver = None

        if ports is None:
            # read from config
            with open('config.yml', 'r') as ymlfile:
                cfg = yaml.load(ymlfile)
                if 'http_port' in cfg['neo4j']:
                    http_uri = "http://{}:{}".format(cfg['neo4j']['host'], cfg['neo4j']['http_port'])
                    logging.debug("Initializing http driver with URI: {}".format(http_uri))
                    self.http_driver = http_gdb(http_uri, username=username, password=password)
        else:
            if 'http' in ports:
                http_uri = "http://{}:{}".format(host, ports['http'])
                self.http_driver = http_gdb(http_uri, username=username, password=password)

    def load(self, start=0, end=None, is_directed=True):
        """
        Read a neo4j database and create a nx graph
        """
        # underscore in numerical notation defined PEP515
        PAGE_SIZE = 10_000

        if end is None:
            count = self.count(is_directed=is_directed)
        else:
            count = end - start

        with click.progressbar(length=count, label='Getting {:,} rows'.format(count)) as bar:
            time_start = self.current_time_in_millis()
            for page in self.get_pages(self.get_edges, start, end, page_size=PAGE_SIZE, is_directed=is_directed):
                self.load_edges(page)
                bar.update(PAGE_SIZE)
            bar.update(count)
            time_end = self.current_time_in_millis()
            logging.debug("time taken to load edges: {} ms".format(time_end - time_start))

        active_node_filters = any(f.filter_local is FilterLocation.NODE for f in self.filters)
        # load_nodes already loads the nodes that belong to the given edges
        # TODO: are these edges being filtered? are their nodes being filtered?
        if active_node_filters:
            for page in self.get_pages(self.get_nodes, start, end):
                time_start = self.current_time_in_millis()
                self.load_nodes(page)
                time_end = self.current_time_in_millis()

    def count(self, is_directed=True):
        """
        Get a page of edges from the database
        """
        direction = '->' if is_directed else '-'
        query = """
        MATCH (s{subject_category}{subject_property})-[p{edge_label}{edge_property}]{direction}(o{object_category}{object_property})
        RETURN COUNT(*) AS count;
        """.format(
            direction=direction,
            **self.build_query_kwargs()
        )

        query_result = self.http_driver.query(query)
        for result in query_result:
            return result[0]

    def load_edges(self, edges):
        start = self.current_time_in_millis()
        for edge in edges:
            self.load_edge(edge)
        end = self.current_time_in_millis()
        logging.debug("time taken to load edges: {} ms".format(end - start))

    def load_nodes(self, nodes):
        start = self.current_time_in_millis()
        for node in nodes:
            self.load_node(node)
        end = self.current_time_in_millis()
        logging.debug("time taken to load nodes: {} ms".format(end - start))

    def load_node(self, node: Node):
        """
        Load node from a neo4j record
        """

        attributes = {}

        for key, value in node.properties.items():
            attributes[key] = value

        if 'category' not in attributes:
            attributes['category'] = list(node.labels)
        else:
            if isinstance(attributes['category'], str):
                attributes['category'] = [attributes['category']]

        if 'Node' not in attributes['category']:
            attributes['category'].append('Node')

        node_id = node['id'] if 'id' in node else node.id

        self.graph.add_node(node_id, attr_dict=attributes)

    def load_edge(self, edge: Relationship):
        """
        Load an edge from a neo4j record
        """

        edge_key = str(uuid.uuid4())
        edge_subject = edge.start
        edge_predicate = edge.properties
        edge_object = edge.end

        subject_id = edge_subject['id'] if 'id' in edge_subject else edge_subject.id
        object_id = edge_object['id'] if 'id' in edge_object else edge_object.id

        attributes = {}

        for key, value in edge_predicate.items():
            attributes[key] = value

        # TODO: Is this code residual from attempting to adapt to several drivers?
        if 'subject' not in attributes:
            attributes['subject'] = subject_id
        if 'object' not in attributes:
            attributes['object'] = object_id
        if 'type' not in attributes:
            attributes['type'] = edge.type
        if 'predicate' not in attributes:
            attributes['predicate'] = attributes['type'] if 'type' in attributes else edge.type

        if not self.graph.has_node(subject_id):
            self.load_node(edge_subject)

        if not self.graph.has_node(object_id):
            self.load_node(edge_object)

        self.graph.add_edge(
            subject_id,
            object_id,
            edge_key,
            attr_dict=attributes
        )

    def build_label(self, label:Union[List[str], str, None]) -> str:
        """
        Takes a potential label and turns it into the string representation
        needed to fill a cypher query.
        """
        if label is None:
            return ''
        elif isinstance(label, str):
            if ' ' in label:
                return f':`{label}`'
            else:
                return f':{label}'
        elif isinstance(label, (list, set, tuple)):
            label = ''.join([self.build_label(l) for l in label])
            return f'{label}'

    def build_properties(self, properties:Dict[str, str]) -> str:
        if properties == {}:
            return ''
        else:
            return ' {{ {properties} }}'.format(properties=self.parse_properties(properties))

    def clean_whitespace(self, s:str) -> str:
        replace = {
            '  ': ' ',
            '\n': ''
        }

        while any(k in s for k in replace.keys()):
            for old_value, new_value in replace.items():
                s = s.replace(old_value, new_value)

        return s.strip()

    def build_query_kwargs(self):
        labels = defaultdict(list)
        for f in self.filters:
            filter_type = f.filter_type
            if filter_type is FilterType.LABEL:
                arg = f.target
                labels[arg].append(f.value)
            else:
                # TODO: Is this error too harsh?
                assert False

        kwargs = {k: '' for k in Filter.targets()}
        for arg, value in labels.items():
            kwargs[arg] = self.build_label(value)

        return kwargs

    def get_pages(self, elements, start=0, end=None, page_size=10_000, **kwargs):
        """
        Gets (end - start) many pages of size page_size.
        """

        # itertools.count(0) starts counting from zero, and would run indefinitely without a return statement.
        # it's distinguished from applying a while loop via providing an index which is formative with the for statement
        for i in itertools.count(0):

            # First halt condition: page pointer exceeds the number of values allowed to be returned in total
            skip = start + (page_size * i)
            limit = page_size if end is None or skip + page_size <= end else end - skip
            if limit <= 0:
                return

            # run a query
            records = elements(skip=skip, limit=limit, **kwargs)

            # Second halt condition: no more data available
            if records:
                """
                * Yield halts execution until next call
                * Thus, the function continues execution upon next call
                * Therefore, a new page is calculated before record is instantiated again
                """
                yield records
            else:
                return

    def get_nodes(self, skip=0, limit=0, tx=None, **kwargs):
        """
        Get a page of nodes from the database
        """

        if limit == 0 or limit is None:
            query = """
            MATCH (n{node_category}{node_property})
            RETURN n
            SKIP {skip};
            """.format(
                skip=skip,
                **self.build_query_kwargs()
            )
        else:
            query = """
            MATCH (n{node_category}{node_property})
            RETURN n
            SKIP {skip} LIMIT {limit};
            """.format(
                skip=skip,
                limit=limit,
                **self.build_query_kwargs()
            )

        query = self.clean_whitespace(query)
        logging.debug(query)

        # Filter out all the associated metadata from the HTTP Driver to ensure the results are clean
        nodeResults = self.http_driver.query(query, returns=(Node))
        nodes = [node for node in nodeResults]
        return nodes

    def get_edges(self, skip=0, limit=0, is_directed=True, tx=None, **kwargs):
        """
        Get a page of edges from the database
        """

        if skip < limit:
            direction = '->' if is_directed else '-'

            # TODO: would it be faster to return only p?
            if limit == 0 or limit is None:
                query = """
                MATCH (s{subject_category}{subject_property})-[p{edge_label}{edge_property}]{direction}(o{object_category}{object_property})
                RETURN s,p,o
                SKIP {skip};
                """.format(
                    skip=skip,
                    direction=direction,
                    **self.build_query_kwargs()
                )
            else:
                query = """
                MATCH (s{subject_category}{subject_property})-[p{edge_label}{edge_property}]{direction}(o{object_category}{object_property})
                RETURN s,p,o
                SKIP {skip} LIMIT {limit};
                """.format(
                    skip=skip,
                    limit=limit,
                    direction=direction,
                    **self.build_query_kwargs()
                )

            query = self.clean_whitespace(query)
            logging.debug(query)

            edgeResults = self.http_driver.query(query, returns=(Node, Relationship, Node))
            edges = [edge for edgeResult in edgeResults for edge in edgeResult if isinstance(edge, Relationship)]
            return edges

        return []

    # TODO: /nneo-5 test from Makefile fails here
    def save_node(self, obj):
        """
        Load a node into neo4j
        """

        if 'id' not in obj:
            raise KeyError("node does not have 'id' property")
        if 'name' not in obj:
            logging.warning("node does not have 'name' property")

        if 'category' not in obj:
            logging.warning("node does not have 'category' property. Using 'Node' as default")
            label = 'Node'
        else:
            label = obj.pop('category')[0]

        properties = ', '.join('n.{0}=${0}'.format(k) for k in obj.keys())
        query = "MERGE (n:{label} {{id: $id}}) SET {properties}".format(label=label, properties=properties)
        self.http_driver.query(query, params=obj)

    def save_node_unwind(self, nodes_by_category, property_names):
        """
        Save all nodes into neo4j using the UNWIND cypher clause
        """

        for category in nodes_by_category.keys():
            self.populate_missing_properties(nodes_by_category[category], property_names)
            query = self.generate_unwind_node_query(category, property_names)

            self.http_driver.query(query, params={'nodes': nodes_by_category[category]})

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
                time_start = self.current_time_in_millis()

                self.http_driver.query(query, params={"relationship": predicate, "edges": subset})
                time_end = self.current_time_in_millis()
                logging.debug("time taken to load edges: {} ms".format(time_end - time_start))

    # expand to include arg for the args that are currently used by formatters/hydrators in bolt_driver?
    def generate_unwind_node_query(self, label, property_names):
        """
        Generate UNWIND cypher clause for a given label and property names (optional)
        """
        ignore_list = ['subject', 'predicate', 'object']

        properties_dict = {p : p for p in property_names if p not in ignore_list}

        properties = ', '.join('n.{0}=node.{0}'.format(k) for k in properties_dict.keys() if k != 'id')

        query = """
        UNWIND $nodes AS node
        MERGE (n:Node {{id: node.id}})
        SET n:{label}, {properties}
        """.format(label=label, properties=properties)

        query = self.clean_whitespace(query)

        logging.debug(query)

        return query

    # expand to include arg for the args that are currently used by formatters/hydrators in bolt_driver?
    def generate_unwind_edge_query(self, relationship, property_names):
        """
        Generate UNWIND cypher clause for a given relationship
        """
        ignore_list = ['subject', 'predicate', 'object']
        properties_dict = {p : "edge.{}".format(p) for p in property_names if p not in ignore_list}

        properties = ', '.join('r.{0}=edge.{0}'.format(k) for k in properties_dict.keys())

        query="""
        UNWIND $edges AS edge
        MATCH (s:Node {{id: edge.subject}}), (o:Node {{id: edge.object}})
        MERGE (s)-[r:{edge_label}]->(o)
        SET {properties}
        """.format(properties=properties, edge_label=relationship)

        query = self.clean_whitespace(query)

        logging.debug(query)

        return query

    def save_edge(self, obj):
        """
        Load an edge into neo4j
        """
        label = obj.pop('predicate')
        subject_id = obj.pop('subject')
        object_id = obj.pop('object')

        properties = ', '.join('r.{0}=${0}'.format(k) for k in obj.keys())

        q="""
        MATCH (s:Node {{id: $subject_id}}), (o:Node {{id: $object_id}})
        MERGE (s)-[r:{label}]->(o)
        SET {properties}
        """.format(properties=properties, label=label)

        q = self.clean_whitespace(q)

        # TODO Is there a reason to pass hydration into the driver?
        params = dict(list(obj.items()) + [("subject_id", subject_id), ("object_id", object_id)])
        self.http_driver.query(q, params=params)

    def save_from_csv(self, nodes_filename, edges_filename):
        """
        Load from a CSV to neo4j
        """
        nodes_df = pd.read_csv(nodes_filename)
        edges_df = pd.read_csv(edges_filename)

        for index, row in nodes_df.iterrows():
            # we can eliminate the need to pass into transactions as 'query' in http_driver uses transactions by default
            self.save_node(row.to_dict())
        for index, row in edges_df.iterrows():
            self.save_edge(row.to_dict())
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

            category = ':'.join(node['category'])
            if category not in nodes_by_category:
                nodes_by_category[category] = [node]
            else:
                nodes_by_category[category].append(node)

            node_properties += [x for x in node if x not in node_properties]

        edges_by_relationship_type = {}
        edge_properties = []
        for n, nbrs in self.graph.adjacency():
            for nbr, eattr in nbrs.items():
                for entry, adjitem in eattr.items():
                    if adjitem['predicate'] not in edges_by_relationship_type:
                        edges_by_relationship_type[adjitem['predicate']] = [adjitem]
                    else:
                        edges_by_relationship_type[adjitem['predicate']].append(adjitem)
                    edge_properties += [x for x in adjitem.keys() if x not in edge_properties]

        self.create_constraints(nodes_by_category.keys())

        self.save_node_unwind(nodes_by_category, node_properties)
        self.save_edge_unwind(edges_by_relationship_type, edge_properties)

    # TODO
    def save(self):
        """
        Load from a nx graph to neo4j
        """

        labels = {'Node'}
        for n in self.graph.nodes():
            node = self.graph.node[n]
            if 'category' in node:
                if isinstance(node['category'], list):
                    labels.update(node['category'])
                else:
                    labels.add(node['category'])


        self.create_constraints(labels)
        for node_id in self.graph.nodes():
            node_attributes = self.graph.node[node_id]
            if 'id' not in node_attributes:
                node_attributes['id'] = node_id
            self.save_node(node_attributes)
        for n, nbrs in self.graph.adjacency():
            for nbr, eattr in nbrs.items():
                for entry, adjitem in eattr.items():
                    self.save_edge(adjitem)
        self.neo4j_report()

    def save_via_apoc(self, nodes_filename=None, edges_filename=None):
        """
        Load from a nx graph to neo4j, via APOC procedure
        """

        if nodes_filename is None or edges_filename is None:
            prefix = uuid.uuid4()
            nodes_filename = "/tmp/{}_nodes.json".format(prefix)
            edges_filename = "/tmp/{}_edges.json".format(prefix)

        self._save_as_json(nodes_filename, edges_filename)
        self.save_nodes_via_apoc(nodes_filename)
        self.save_edges_via_apoc(edges_filename)
        self.update_node_labels()

    def save_nodes_via_apoc(self, filename):
        """
        Load nodes from a nx graph to neo4j, via APOC procedure
        """
        logging.info("reading nodes from {} and saving to Neo4j via APOC procedure".format(filename))
        start = self.current_time_in_millis()
        query = """
        CALL apoc.periodic.iterate(
            "CALL apoc.load.json('file://""" + filename + """') YIELD value AS jsonValue RETURN jsonValue",
            "MERGE (n:Node {id:jsonValue.id}) set n+=jsonValue",
            {
                batchSize: 10000,
                iterateList: true
            }
        )
        """

        # TODO: Parameterize use of bolt driver vs https for queries?
        self.http_driver.query(query)
        end = self.current_time_in_millis()
        logging.debug("time taken for APOC procedure: {} ms".format(end - start))

    def save_edges_via_apoc(self, filename):
        """
        Load edges from a nx graph to neo4j, via APOC procedure
        """
        logging.info("reading edges from {} and saving to Neo4j via APOC procedure".format(filename))
        start = self.current_time_in_millis()
        query = """
        CALL apoc.periodic.iterate(
            "CALL apoc.load.json('file://""" + filename + """') YIELD value AS jsonValue return jsonValue",
            "MATCH (a:Node {id:jsonValue.subject})
            MATCH (b:Node {id:jsonValue.object})
            CALL apoc.merge.relationship(a,jsonValue.predicate,{id:jsonValue.id},jsonValue,b) YIELD rel
            RETURN count(*) as relationships",
            {
                batchSize: 10000,
                iterateList: true
            }
        );
        """

        # TODO: Parameterize use of bolt driver vs https for queries?
        self.http_driver.query(query)
        end = self.current_time_in_millis()
        logging.debug("time taken for APOC procedure: {} ms".format(end - start))

    # TODO
    def update_node_labels(self):
        """
        Update node labels
        """
        logging.info("updating node labels")
        start = self.current_time_in_millis()
        query_string = """
        UNWIND $nodes as node
        MATCH (n:Node {{id: node.id}}) SET n:{node_labels}
        """

        nodes_by_category = {}
        for node in self.graph.nodes(data=True):
            node_data = node[1]
            key = ':'.join(node_data['category'])
            if key in nodes_by_category:
                nodes_by_category[key].append(node_data)
            else:
                nodes_by_category[key] = [node_data]

        for category in nodes_by_category:
            query = query_string.format(node_labels=category)
            self.http_driver.query(query, params={"nodes": nodes_by_category[category]})
        end = self.current_time_in_millis()
        logging.debug("time taken to update node labels: {} ms".format(end - start))

    def report(self):
        logging.info("Total number of nodes: {}".format(len(self.graph.nodes())))
        logging.info("Total number of edges: {}".format(len(self.graph.edges())))

    def neo4j_report(self):
        """
        Give a summary on the number of nodes and edges in neo4j database
        """
        for r in self.http_driver.query("MATCH (n) RETURN COUNT(*)"):
            logging.info("Number of Nodes: {}".format(r[0]))
        for r in self.http_driver.query("MATCH (s)-->(o) RETURN COUNT(*)"):
            logging.info("Number of Edges: {}".format(r[0]))

    # TODO
    def create_constraints(self, labels):
        """
        Create a unique constraint on node 'id' for all labels
        """
        query = "CREATE CONSTRAINT ON (n:{}) ASSERT n.id IS UNIQUE"
        label_set = set()

        for label in labels:
            if ':' in label:
                sub_labels = label.split(':')
                for sublabel in sub_labels:
                    label_set.add(sublabel)
            else:
                label_set.add(label)

        for label in label_set:
            self.http_driver.query(query.format(label))

    def _save_as_json(self, node_filename, edge_filename):
        """
        Write a graph as JSON (used internally)
        """
        nodes = self._save_nodes_as_json(node_filename)
        edges = self._save_edges_as_json(edge_filename)

    def _save_nodes_as_json(self, filename):
        """
        Write nodes as JSON (used internally)
        """
        FH = open(filename, "w")
        nodes = []
        for node in self.graph.nodes(data=True):
            nodes.append(node[1])

        FH.write(json.dumps(nodes))
        FH.close()

    def _save_edges_as_json(self, filename):
        """
        Write edges as JSON (used internally)
        """
        fh = open(filename, "w")
        edges = []
        for edge in self.graph.edges_iter(data=True, keys=True):
            edges.append(edge[3])

        fh.write(json.dumps(edges))
        fh.close()

    @staticmethod
    def parse_properties(properties, delim = '|'):
        property_list = []
        for key in properties:
            if key in ['subject', 'predicate', 'object']:
                continue

            values = properties[key]
            if type(values) == type(""):
                pair = "{}: \"{}\"".format(key, str(values))
            else:
                pair = "{}: {}".format(key, str(values))
            property_list.append(pair)
        return ','.join(property_list)

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
    pass

