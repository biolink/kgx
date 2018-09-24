import networkx as nx
from typing import Union, List, Dict
from .prefix_manager import PrefixManager
import logging

from metamodel.utils.schemaloader import SchemaLoader

class Validator(object):
    """
    Object for validating a property graph
    """

    def __init__(self):
        self.prefix_manager = PrefixManager()
        self.items = set()
        self.errors = []
        self.schema = SchemaLoader('https://biolink.github.io/biolink-model/biolink-model.yaml').resolve()

    def ok(self):
        return len(self.errors) == 0

    def validate(self, G):
        """
        Validate a property graph

        Test all node and edge properties plus relationship types are declared
        """
        for nid,ad in G.nodes_iter(data=True):
            self.validate_node(nid, ad)
        for oid,sid,ad in G.edges_iter(data=True):
            self.validate_edge(G, oid, sid, ad)

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

    def report(self, item, info=""):
        if item in self.items:
            return
        msg = "Item: {} Message: {}".format(item, info)
        logging.error(msg)
        self.errors.append(msg)
        self.items.add(item)
