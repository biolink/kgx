import logging
from typing import Optional, Tuple

import networkx as nx
import stringcase

from kgx.utils.kgx_utils import generate_edge_key, get_toolkit, snakecase_to_sentencecase, sentencecase_to_snakecase

SAME_AS = 'same_as'
LEADER_ANNOTATION = 'clique_leader'

# TODO: Get the prefix priority order from BioLink Model
PREFIX_PRIORITIZATION_MAP = {
    'gene': ['HGNC', 'NCBIGene', 'Ensembl']
}

MAPPING = {}

class CliqueMerge(object):
    """

    """

    def __init__(self, prefix_prioritization_map: dict = None):
        self.toolkit = get_toolkit()
        self.clique_graph = nx.Graph()
        self.target_graph = None
        if prefix_prioritization_map:
            for x, v in prefix_prioritization_map:
                PREFIX_PRIORITIZATION_MAP[x] = v

    def build_cliques(self, target_graph: nx.MultiDiGraph):
        """
        Builds a clique graph from `same_as` edges in `target_graph`.

        Parameters
        ----------
        target_graph: networkx.MultiDiGraph
            A MultiDiGraph that contains nodes and edges

        Returns
        -------
        networkx.Graph
            The clique graph with only `same_as` edges

        """
        self.target_graph = target_graph
        for u, v, data in target_graph.edges(data=True):
            if 'edge_label' in data and data['edge_label'] == SAME_AS:
                # load all same_as edges to self.clique_graph
                self.clique_graph.add_node(u, **target_graph.node[u])
                self.clique_graph.add_node(v, **target_graph.node[v])
                self.clique_graph.add_edge(u, v, **data)

    def update_categories(self, clique: list):
        """
        For a given clique, get category for each node in clique and validate against BioLink Model,
        mapping to BioLink Model category where needed.

        Ex.: If a node has `gene` as its category, then this method adds all of its ancestors.

        Parameters
        ----------
        clique: list
            A list of nodes from a clique

        """
        updated_node_categories = {}
        for node in clique:
            data = self.clique_graph.node[node]
            print(data)
            if 'category' in data:
                categories = data['category']
            else:
                # get category from equivalence
                categories = self.get_category_from_equivalence(node, data)

            extended_categories = set()
            invalid_categories = []
            for category in categories:
                # TODO: this sentence case conversion needs to be handled properly
                category = snakecase_to_sentencecase(category).lower()
                logging.debug("Looking at category: {}".format(category))
                element = self.toolkit.get_element(category)
                if element:
                    # category exists in BioLink Model as a class or as an alias to a class
                    mapped_category = element['name']
                    ancestors = self.toolkit.ancestors(mapped_category)
                    if len(ancestors) > len(extended_categories):
                        # the category with the longest list of ancestors will be the most specific category
                        logging.debug("Ancestors for {} is larger than previous one".format(mapped_category))
                        extended_categories = ancestors
                else:
                    logging.warning("[1] category '{}' not in BioLink Model".format(category))
                    invalid_categories.append(category)
            logging.debug("Invalid categories: {}".format(invalid_categories))
            extended_categories = [stringcase.snakecase(x).lower() for x in extended_categories]

            for x in categories:
                element = self.toolkit.get_element(x)
                if element is None:
                    logging.warning("[2] category '{}' is not in BioLink Model".format(x))
                    continue
                mapped_category = element['name']
                if stringcase.snakecase(mapped_category).lower() not in extended_categories:
                    logging.warning("category '{}' not in ancestor closure: {}".format(stringcase.snakecase(mapped_category).lower(), extended_categories))
                    mapped = MAPPING[x] if x in MAPPING.keys() else x
                    if mapped not in extended_categories:
                        logging.warning("category '{}' is not even in any custom defined mapping. ".format(mapped_category))
                        invalid_categories.append(x)

            update_dict = {'category': extended_categories}
            if invalid_categories:
                update_dict['_invalid_category'] = invalid_categories
            updated_node_categories[node] = update_dict
        logging.debug("Updating nodes in clique with: {}".format(updated_node_categories))
        nx.set_node_attributes(self.clique_graph, updated_node_categories)
        nx.set_node_attributes(self.target_graph, updated_node_categories)

    def validate_categories(self, clique: list) -> Tuple[str, list]:
        """
        For nodes in a clique, validate the category for each node to make sure that all nodes in a clique
        are of the same type.

        Parameters
        ----------
        clique: list
            A list of nodes from a clique

        Returns
        -------
        tuple[str, list]
            A tuple of clique category string and a list of invalid nodes

        """
        invalid_nodes = []
        all_categories = []
        for node in clique:
            logging.info(node)
            all_categories.append(self.clique_graph.node[node]['category'][0])

        (clique_category, clique_category_ancestors) = self.get_the_most_specific_category(all_categories)
        logging.debug("Most specific category: {}".format(clique_category))
        logging.debug("Most specific category ancestors: {}".format(clique_category_ancestors))
        for node in clique:
            data = self.clique_graph.node[node]
            node_category = data['category'][0]
            logging.debug("node_category: {}".format(node_category))
            # TODO: this sentencecase to snakecase transition needs to be handled properly
            ancestors = [sentencecase_to_snakecase(x) for x in clique_category_ancestors]
            logging.debug("clique ancestors: {}".format(ancestors))
            if node_category not in ancestors:
                invalid_nodes.append(node)
                logging.info("clique category '{}' does not match node: {}".format(clique_category, data))
            # TODO: check if node category is a subclass of any of the ancestors via other ontologies
        logging.info("Invalid Nodes: {}".format(invalid_nodes))
        return clique_category, invalid_nodes

    def get_the_most_specific_category(self, categories: list) -> Tuple[str, list]:
        """
        From a list of categories, it tries to fetch ancestors for all.
        The category with the longest ancestor is considered to be the most specific.

        Parameters
        ----------
        categories: list
            A list of categories

        Returns
        -------
        tuple[str, list]
            A tuple of the most specific category and a list of ancestors of that category

        """
        # TODO: could be integrated into update_categories method
        most_specific_category = None
        most_specific_category_ancestors = []
        for category in categories:
            logging.debug("category: {}".format(category))
            formatted_category = snakecase_to_sentencecase(category)
            logging.debug("formatted_category: {}".format(formatted_category))
            element = self.toolkit.get_element(category)
            if element:
                # category exists in BioLink Model as a class or as an alias to a class
                mapped_category = element['name']
                ancestors = self.toolkit.ancestors(mapped_category)
                logging.debug("ancestors: {}".format(ancestors))
                if len(ancestors) > len(most_specific_category_ancestors):
                    # the category with the longest list of ancestors will be the most specific category
                    most_specific_category = category
                    most_specific_category_ancestors = ancestors
        return most_specific_category, most_specific_category_ancestors

    def elect_leader(self):
        """
        Elect leader for each clique in a graph.

        """
        cliques = list(nx.connected_components(self.clique_graph))

        election_strategy = None
        for clique in cliques:
            clique_category = None
            logging.info("Processing clique: {}".format(clique))
            # first update all categories for nodes in a clique
            self.update_categories(clique)
            # validate categories of all nodes in a clique, while removing the ones that are not supposed to be in the clique
            (clique_category, invalid_nodes) = self.validate_categories(clique)
            if invalid_nodes:
                logging.debug("Removing nodes {} as they are not supposed to be part of clique: {}".format(invalid_nodes, clique))
                clique = [x for x in clique if x not in invalid_nodes]
                for n in invalid_nodes:
                    self.clique_graph.remove_node(n)
                    # TODO: what about the original equivalentClass edge that made this incorrect assertion?

            leader = None
            # First check for LEADER_ANNOTATION property
            (leader, election_strategy) = self.get_leader_by_annotation(clique)

            if leader is None:
                # If leader is None, then use prefix prioritization
                logging.debug("Could not elect clique leader by looking for LEADER_ANNOTATION property; Using prefix prioritization instead")
                # assuming that all nodes in a clique belong to the same category
                if clique_category in PREFIX_PRIORITIZATION_MAP.keys():
                    (leader, election_strategy) = self.get_leader_by_prefix_priority(clique, PREFIX_PRIORITIZATION_MAP[clique_category])
                else:
                    logging.debug("No prefix order found for category '{}' in PREFIX_PRIORITIZATION_MAP".format(clique_category))

            if leader is None:
                # If leader is still None then fall back to alphabetical sort on prefixes
                logging.info("Could not elect clique leader by PREFIX_PRIORITIZATION; Using alphabetical sort on prefixes")
                (leader, election_strategy) = self.get_leader_by_sort(clique)

            logging.debug("Elected {} as leader via {} for clique {}".format(leader, election_strategy, clique))
            self.clique_graph.node[leader][LEADER_ANNOTATION] = True
            self.target_graph.node[leader][LEADER_ANNOTATION] = True
            self.clique_graph.node[leader]['election_strategy'] = election_strategy
            self.target_graph.node[leader]['election_strategy'] = election_strategy

    def get_leader_by_annotation(self, clique: list) -> Tuple[Optional[str], Optional[str]]:
        """
        Get leader by searching for leader annotation property in any of the nodes in a given clique.

        Parameters
        ----------
        clique: list
            A list of nodes from a clique

        Returns
        -------
        tuple[Optional[str], Optional[str]]
            A tuple containing the node that has been elected as the leader, and the election strategy

        """
        leader = None
        election_strategy = None
        for node in clique:
            attributes = self.clique_graph.node[node]
            if LEADER_ANNOTATION in attributes and eval(attributes[LEADER_ANNOTATION]):
                logging.debug("Node {} in clique has LEADER_ANNOTATION property; electing it as clique leader".format(node))
                election_strategy = 'LEADER_ANNOTATION'
        return leader, election_strategy

    def get_leader_by_prefix_priority(self, clique: list, prefix_priority_list: list) -> Tuple[Optional[str], Optional[str]]:
        """
        Get leader from clique based on a given prefix priority.

        Parameters
        ----------
        clique: list
            A list of nodes that correspond to a clique
        prefix_priority_list: list
            A list of prefixes in descending priority

        Returns
        -------
        tuple[Optional[str], Optional[str]]
            A tuple containing the node that has been elected as the leader, and the election strategy

        """
        leader = None
        election_strategy = None
        for prefix in prefix_priority_list:
            logging.debug("Checking for prefix {} in {}".format(prefix, clique))
            leader = next((s for s in clique if prefix in s), None)
            if leader:
                election_strategy = "PREFIX_PRIORITIZATION"
                break
        return leader, election_strategy

    def get_leader_by_sort(self, clique: list) -> Tuple[Optional[str], Optional[str]]:
        """
        Get leader from clique based on the first selection from an alphabetical sort of the node id prefixes.

        Parameters
        ----------
        clique: list
            A list of nodes that correspond to a clique

        Returns
        -------
        tuple[Optional[str], Optional[str]]
            A tuple containing the node that has been elected as the leader, and the election strategy

        """
        election_strategy = 'ALPHABETICAL_SORT'
        prefixes = [x.split(':', 1)[0] for x in clique]
        prefixes.sort()
        leader_prefix = prefixes[0]
        [leader] = [x for x in clique if leader_prefix in x]
        return leader, election_strategy

    def consolidate_edges(self) -> nx.MultiDiGraph:
        """
        Move all edges from nodes in a clique to the clique leader.

        Returns
        -------
        nx.MultiDiGraph
            The target graph where all edges from nodes in a clique are moved to clique leader

        """
        cliques = list(nx.connected_components(self.clique_graph))
        for clique in cliques:
            logging.info("processing clique: {}".format(clique))
            leader = [x for x in clique if LEADER_ANNOTATION in self.clique_graph.node[x] and self.clique_graph.node[x][LEADER_ANNOTATION]]
            if len(leader) == 0:
                logging.debug("No leader for clique {}; skipping".format(clique))
                continue
            else:
                leader = leader[0]
            nx.set_node_attributes(self.target_graph, {leader: {LEADER_ANNOTATION: self.clique_graph.node[leader].get(LEADER_ANNOTATION), 'election_strategy': self.clique_graph.node[leader].get('election_strategy')}})
            for node in clique:
                if node == leader:
                    continue
                in_edges = self.target_graph.in_edges(node, True)
                filtered_in_edges = [x for x in in_edges if x[2]['edge_label'] != SAME_AS]
                equiv_in_edges = [x for x in in_edges if x[2]['edge_label'] == SAME_AS]
                logging.debug("Moving {} in-edges from {} to {}".format(len(in_edges), node, leader))
                for u, v, edge_data in filtered_in_edges:
                    key = generate_edge_key(u, edge_data['edge_label'], v)
                    self.target_graph.remove_edge(u, v, key=key)
                    edge_data['_original_subject'] = edge_data['subject']
                    edge_data['_original_object'] = edge_data['object']
                    edge_data['object'] = leader
                    key = generate_edge_key(u, edge_data['edge_label'], v)
                    self.target_graph.add_edge(edge_data['subject'], edge_data['object'], key, **edge_data)

                out_edges = self.target_graph.out_edges(node, True)
                filtered_out_edges = [x for x in out_edges if x[2]['edge_label'] != SAME_AS]
                equiv_out_edges = [x for x in out_edges if x[2]['edge_label'] == SAME_AS]
                logging.debug("Moving {} out-edges from {} to {}".format(len(out_edges), node, leader))
                for u, v, edge_data in filtered_out_edges:
                    key = generate_edge_key(u, edge_data['edge_label'], v)
                    self.target_graph.remove_edge(u, v, key=key)
                    edge_data['_original_subject'] = edge_data['subject']
                    edge_data['_original_object'] = edge_data['object']
                    edge_data['subject'] = leader
                    key = generate_edge_key(u, edge_data['edge_label'], v)
                    self.target_graph.add_edge(edge_data['subject'], edge_data['object'], key, **edge_data)

                aliases = self.target_graph.node[leader].get('aliases') if 'aliases' in self.target_graph.node[leader] else []

                for u, v, edge_data in equiv_in_edges:
                    if u != leader:
                        aliases.append(u)
                    if v != leader:
                        aliases.append(v)
                    self.target_graph.remove_edge(u, v, key=generate_edge_key(u, SAME_AS, v))

                logging.debug("equiv out edges: {}".format(equiv_out_edges))
                for u, v, edge_data in equiv_out_edges:
                    if u != leader:
                        logging.debug("{} is an alias of leader {}".format(u, leader))
                        aliases.append(u)
                    if v != leader:
                        logging.debug("{} is an alias of leader {}".format(v, leader))
                        aliases.append(v)
                    self.target_graph.remove_edge(u, v, key=generate_edge_key(u, SAME_AS, v))

                # set aliases for leader
                nx.set_node_attributes(self.target_graph, {leader: {'aliases': aliases}})
                # remove all node instances of aliases
                self.target_graph.remove_nodes_from(aliases)

        return self.target_graph

    def get_category_from_equivalence(self, node: str, attributes: dict) -> str:
        """
        Get category for a node based on its equivalent nodes in a graph.

        Parameters
        ----------
        node: str
            Node identifier
        attributes: dict
            Node's attributes

        Returns
        -------
        str
            Category for the node

        """
        category = []
        for u, v, data in self.clique_graph.edges(node, data=True):
            if data['edge_label'] == 'same_as':
                if u == node:
                    category = self.clique_graph.nodes[v]['category']
                    break
                elif v == node:
                    category = self.clique_graph.nodes[u]['category']
                    break
                update = {node: {'category': category}}
                nx.set_node_attributes(self.clique_graph, update)

        return category
