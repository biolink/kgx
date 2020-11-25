import rdflib
from rdflib import URIRef
from typing import Set, List, Dict, Generator, Optional

from pystache import render
from SPARQLWrapper import SPARQLWrapper, JSON, POSTDIRECTLY
from itertools import zip_longest

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.transformers.transformer import Transformer
from kgx.transformers.rdf_graph_mixin import RdfGraphMixin

log = get_logger()


class SparqlTransformer(RdfGraphMixin, Transformer):
    """
    Transformer for communicating with a SPARQL endpoint.

    Parameters
    ----------
    source_graph: Optional[kgx.graph.base_graph.BaseGraph]
        The source graph
    url: Optional[str]
        The URL to a SPARQL endpoint

    """

    # TODO: fix query
    edge_query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    SELECT * WHERE {
        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{{predicate}}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{{subject_category}}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{{object_category}}} .
        {{/object_category}}

        ?subject ?predicate ?object .
    }

    """

    def __init__(self, source_graph: Optional[BaseGraph] = None, url: Optional[str] = None):
        super().__init__(source_graph)
        self.url = url
        raise NotImplementedError("This class has not yet been implemented.")

    def load_graph(self, rdfgraph: rdflib.Graph, predicates: Optional[Set[URIRef]] = None, **kwargs: Dict) -> None:
        """
        Fetch triples from the SPARQL endpoint and load them as edges.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            A rdflib Graph (unused)
        predicates: Optional[Set[URIRef]]
            A set containing predicates in rdflib.URIRef form
        kwargs: Dict
            Any additional arguments.

        """
        if predicates:
            for predicate in predicates:
                predicate = '<{}>'.format(predicate)
                q = render(self.edge_query, {'predicate': predicate})
                results = self.query(q)
                for r in results:
                    s = r['subject']['value']
                    p = r['predicate']['value']
                    o = r['object']['value']
                    if r['object']['type'] == 'literal':
                        self.add_node_attribute(s, key=p, value=o)
                    else:
                        self.add_edge(s, o, p)

    def query(self, q: str) -> Dict:
        """
        Query a SPARQL endpoint.

        Parameters
        ----------
        q: str
            The query string

        Returns
        -------
        Dict
            A dictionary containing results from the query

        """
        sparql = SPARQLWrapper(self.url)
        sparql.setQuery(q)
        sparql.setReturnFormat(JSON)
        log.info("Query: {}".format(q))
        results = sparql.query().convert()
        bindings = results['results']['bindings']
        log.info("Rows fetched: {}".format(len(bindings)))
        return bindings


class MonarchSparqlTransformer(SparqlTransformer):
    """
    TODO
    """

    # OBAN-specific query
    edge_query = """
    SELECT ?subject ?predicate ?object ?prop ?val WHERE {
        ?a a Association: ;
           subject: ?subject ;
           predicate: ?predicate ;
           subject: ?object ;
           ?prop ?val .

        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{{predicate}}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{{subject_category}}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{{object_category}}} .
        {{/object_category}}
    }
    """

    def __init__(self, source_graph: BaseGraph = None):
        super().__init__(source_graph)
        raise NotImplementedError("This class has not yet been implemented.")


class RedSparqlTransformer(SparqlTransformer):
    """
    Transformer for communicating with Data2Services Knowledge Graph, a.k.a. Translator Red KG.
    """

    count_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX bl: <http://w3id.org/biolink/vocab/>

    SELECT (COUNT(*) AS ?triples)
    WHERE {

        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{{predicate}}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{{subject_category}}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{{object_category}}} .
        {{/object_category}}

        ?a rdf:type {{{association}}} ;
           bl:subject ?subject ;
           bl:relation ?predicate ;
           bl:object ?object ;
           ?edge_property_key ?edge_property_value .
    }
    """

    edge_query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX bl: <http://w3id.org/biolink/vocab/>

    SELECT ?subject ?predicate ?object ?edge_property_key ?edge_property_value
    WHERE {

        {{#predicate}}
        ?predicate rdfs:subPropertyOf* {{{predicate}}} .
        {{/predicate}}

        {{#subject_category}}
        ?subject (rdf:type?/rdfs:subClassOf*) {{{subject_category}}} .
        {{/subject_category}}

        {{#object_category}}
        ?object (rdf:type?/rdfs:subClassOf*) {{{object_category}}} .
        {{/object_category}}

        ?a rdf:type {{{association}}} ;
           bl:subject ?subject ;
           bl:relation ?predicate ;
           bl:object ?object ;
           ?edge_property_key ?edge_property_value .
    }
    ORDER BY ?subject ?predicate ?object
    OFFSET {{offset}}
    LIMIT {{limit}}
    """

    get_node_properties_query = """
    PREFIX bl: <http://w3id.org/biolink/vocab/>

    SELECT ?subject ?predicate ?object
    WHERE
    {{
        ?subject ?predicate ?object
        VALUES ?subject {{
            {curie_list}
        }}
    }}

    """

    IS_DEFINED_BY = "Team Red"

    def __init__(self, source_graph: BaseGraph = None, url: str ='http://graphdb.dumontierlab.com/repositories/ncats-red-kg'):
        super().__init__(source_graph, url)
        raise NotImplementedError("This class has not yet been implemented.")

    def load_graph(self, rdfgraph: rdflib.Graph, predicates: Optional[Set[URIRef]] = None, **kwargs: Dict) -> None:
        """
        Fetch all triples using the specified predicates
        and add them to an instance of BaseGraph.

        Parameters
        ----------
        rdfgraph: rdflib.Graph
            A rdflib Graph (unused)
        predicates: Optional[Set[URIRef]]
            A set containing predicates in rdflib.URIRef form
        kwargs: dict
            Any additional arguments.
            Ex: specifying 'limit' argument will limit the number of triples fetched.

        """
        if predicates:
            for predicate in predicates:
                sparql = SPARQLWrapper(self.url)
                association = '<{}>'.format(predicate)
                query = render(self.count_query, {'association': association})
                log.debug(query)
                sparql.setQuery(query)
                sparql.setReturnFormat(JSON)
                results = sparql.query().convert()
                count = int(results['results']['bindings'][0]['triples']['value'])
                log.info("Expected triples for query: {}".format(count))
                step = 1000
                start = 0
                for i in range(step, count + step, step):
                    end = i
                    query = render(self.edge_query, {'association': association, 'offset': start, 'limit':step})
                    sparql.setQuery(query)
                    log.debug("Fetching triples with predicate {}".format(predicate))
                    results = sparql.query().convert()
                    node_list = set()
                    for r in results['results']['bindings']:
                        node_list.add("<{}>".format(r['subject']['value']))
                        node_list.add("<{}>".format(r['object']['value']))
                    start = end
                    self.load_nodes(node_list)
                    for r in results['results']['bindings']:
                        s = r['subject']['value']
                        p = r['predicate']['value']
                        o = r['object']['value']
                        self.add_edge(s, o, p)
                        # TODO: preserve edge properties

            self.categorize()

    def categorize(self) -> None:
        """
        Checks for a node's category property and assigns a category from BioLink Model.
        TODO: categorize for edges?
        """
        for n, data in self.graph.nodes(data=True):
            if 'category' not in data and 'type' in data:
                data['category'] = data['type'].replace('biolink:', '')

    def load_nodes(self, node_set: Set) -> None:
        """
        Load nodes into an instance of BaseGraph.

        This method queries the SPARQL endpoint for all triples where nodes in the
        node_set is a subject.

        Parameters
        ----------
        node_set: list
            A list of node CURIEs

        """
        node_generator = self._grouper(node_set, 10000)
        nodes = next(node_generator, None)
        while nodes is not None:
            log.info("Fetching properties for {} nodes".format(len(nodes)))
            nodes = filter(None, nodes)
            # TODO: is there a better way to fetch node properties?
            query = self.get_node_properties_query.format(curie_list=' '.join(nodes))
            log.info(query)
            sparql = SPARQLWrapper(self.url)
            sparql.setRequestMethod(POSTDIRECTLY)
            sparql.setMethod("POST")
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            node_results = sparql.query().convert()
            d: Dict = {}
            for r in node_results['results']['bindings']:
                if r['object']['type'] != 'bnode':
                    subject = r['subject']['value']
                    object = r['object']['value']
                    predicate = r['predicate']['value']
                    if predicate.startswith('bl:'):
                        predicate = predicate.split(':')[1]
                    if subject not in d:
                        d[subject] = {}
                    d[subject][predicate] = object

            for node, attr_dict in d.items():
                for key, value in attr_dict.items():
                    self.add_node_attribute(node, key=key, value=value)
            d.clear()
            nodes = next(node_generator, None)

    @staticmethod
    def _grouper(iterable: Set, n, fillvalue: str = None) -> Generator:
        """
        Collect data into fixed-length chunks.

        Parameters
        ----------
        iterable: set
            A set to group
        n: int
            Size of a chunk
        fillvalue: str
            When chunking, if the last chunk contains less than n then what
            value to use to fill the missing values
        """
        yield from zip_longest(*[iter(iterable)] * n, fillvalue=fillvalue)
