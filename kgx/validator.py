import networkx as nx
import bmt
from typing import Union, List, Dict
from .prefix_manager import PrefixManager
import logging
import click
import re

from collections import defaultdict

from metamodel.utils.schemaloader import SchemaLoader

class Validator(object):
    """
    Object for validating a property graph
    """

    def __init__(self, record_size=None):
        self.prefix_manager = PrefixManager()
        self.items = set()
        self.errors = []
        self.schema = SchemaLoader('https://biolink.github.io/biolink-model/biolink-model.yaml').resolve()
        self.record_size = record_size
        self.error_dict = defaultdict(set)

    def ok(self):
        return len(self.errors) == 0

    def validate(self, G):
        """
        Validate a property graph

        Test all node and edge properties plus relationship types are declared
        """
        self.validate_categories(G)
        self.validate_edge_labels(G)
        self.validate_node_properties(G)
        # for nid,ad in G.nodes(data=True):
        #     self.validate_node(nid, ad)
        # for oid,sid,ad in G.edges(data=True):
        #     self.validate_edge(G, oid, sid, ad)

    def validate_node(self, nid, ad):
        self.validate_id(nid)
        self.validate_props(ad)
        self.validate_node_requirements(ad)

    def validate_edge(self, G, oid, sid, ad):
        self.validate_id(oid)
        self.validate_id(sid)
        self.validate_props(ad)
        self.validate_edge_requirements(G, oid, sid, ad)

    def validate_id(self, id):
        if ":" in id:
            uri = self.prefix_manager.expand(id)
            if uri is None or uri == id:
                self.report(id, "expansion is identical")
        else:
            if id not in self.prefix_manager.prefixmap:
                self.report(id, "no such short form")

    def validate_node_requirements(self, ad):
        node_id = ad.get('id')

        self.test(lambda: 'id' in ad, node_id, 'node lacks id attribute')
        self.test(lambda: 'name' in ad, node_id, 'node lacks name attribute')
        success = self.test(lambda: 'category' in ad, node_id, 'node lacks category attribute')

        if not success:
            return

        category = ad['category']

        if isinstance(category, str):
            self.test(lambda: category in self.schema.classes, category, 'node category is invalid')
        elif isinstance(category, (list, tuple, set)):
            for c in category:
                self.test(lambda: c in self.schema.classes, c, 'node category is invalid')
        else:
            self.report(edge_label, f'category is invalid type: {type(category)}')

        labels = ad.get('labels')

        if labels is not None:
            for label in labels:
                if label not in self.schema.classes:
                    self.report(label, 'node label is invalid')

    def validate_edge_requirements(self, G, oid, sid, ad):
        """
        Checks that an edge has an edge_label, that it's valid and within the
        minimal list, and that the subject and object fall within the edge's
        domain and range.
        """
        edge_id = ad.get('id')

        self.test(lambda: 'is_defined_by' in ad, edge_id, 'edge lacks is_defined_by attribute')
        self.test(lambda: 'provided_by' in ad, edge_id, 'edge lacks provided_by attribute')

        success = self.test(lambda: 'edge_label' in ad, edge_id, 'edge lacks edge_label attribute')

        if not success:
            return

        edge_label = ad['edge_label']

        if not isinstance(edge_label, str):
            self.report(edge_label, f'edge label is invalid type: {type(edge_label)}')
            return

        if edge_label not in self.schema.slots:
            self.report(edge_label, 'edge label is invalid')
            return

        slot = self.schema.slots[edge_label]

        fn = lambda: 'in_subset' in slot and 'translator_minimal' in slot['in_subset']
        self.test(fn, edge_label, 'edge label not in minimal list')

        object_category = G.node[oid]['category']
        subject_category = G.node[sid]['category']

        if slot.domain is not None:
            if slot.domain != subject_category:
                self.report(sid, f'{subject_category} is outside of domain of {edge_label}')

        if slot.range is not None:
            if slot.range != object_category:
                self.report(oid, f'{object_category} is outside of domain of {edge_label}')

    def validate_props(self, ad):
        for p,v in ad.items():
            self.validate_id(p)

    def test(self, fn, item, info=""):
        if not fn():
            self.report(item, info)
            return False
        else:
            return True

    def validate_categories(self, G):
        with click.progressbar(G.nodes(data=True)) as bar:
            for n, data in bar:
                categories = data.get('category')
                if categories is None:
                    self.log_node_error(n, 'absent category')
                elif not isinstance(categories, list):
                    self.log_node_error(n, 'invalid category type', message='category type is {} when it should be {}'.format(type(categories), list))
                else:
                    for category in categories:
                        c = bmt.get_class(category)
                        if c is None:
                            self.log_node_error(n, 'invalid category', message='{} not in biolink model'.format(category))
                        elif category != c.name and category in c.aliases:
                            self.log_node_error(n, 'alias category', message='should not use alias {} for {}'.format(c.name, category))

    def validate_edge_labels(self, G):
        with click.progressbar(G.edges(data=True)) as bar:
            for u, v, data in bar:
                edge_label = data.get('edge_label')
                if edge_label is None:
                    self.log_edge_error(u, v, 'absent edge label')
                elif not isinstance(edge_label, str):
                    self.log_edge_error(u, v, 'invalid edge label type', message='edge label type is {} when it should be {}'.format(type(edge_label), str))
                else:
                    p = bmt.get_predicate(edge_label)
                    if p is None:
                        self.log_edge_error(u, v, 'invalid edge label', message='{} not in biolink model'.format(edge_label))
                    elif edge_label != p.name and edge_label in p.aliases:
                        self.log_edge_error(u, v, 'alias edge label', message='should not use alias {} for {}'.format(p.name, edge_label))
                    elif not re.match(r'^[a-z_]*$', edge_label):
                        self.log_edge_error(u, v, 'invalid edge label', message='"{}" is not snake case'.format(edge_label))

    def validate_node_properties(self, G):
        named_thing = bmt.get_class('named thing')
        with click.progressbar(G.nodes(data=True)) as bar:
            for n, data in bar:
                for key, value in data.items():
                    if key in named_thing.slots:
                        if bmt.get_element(key).multivalued and not isinstance(value, list):
                            self.log_node_error(n, 'invalid property type', message='{} type should be {} but its {}'.format(key, list, type(value)))
                        if not bmt.get_element(key).multivalued and isinstance(value, list):
                            self.log_node_error(n, 'invalid property type', message='{} type should be {} but its {}'.format(key, str, type(value)))
                if not re.match(r'^[^ :]+:[^ :]+$', n):
                    self.log_node_error(n, 'invalid property value', message='id is not a curie')

    def log_edge_error(self, u, v, error_type, *, message=None):
        if self.record_size is None or len(self.error_dict[error_type]) < self.record_size:
            self.error_dict[error_type].add((u, v, message))

    def log_node_error(self, n, error_type, *, message=None):
        if self.record_size is None or len(self.error_dict[error_type]) < self.record_size:
            self.error_dict[error_type].add((n, message))

    def report(self, item, info=""):
        if item in self.items:
            return
        msg = "Item: {} Message: {}".format(item, info)
        logging.error(msg)
        self.errors.append(msg)
        self.items.add(item)
