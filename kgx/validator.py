import networkx as nx
from typing import Union, List, Dict
from .prefix_manager import PrefixManager
import logging

class Validator(object):
    """
    Object for validating a property graph
    """
    
    def __init__(self):
        self.prefix_manager = PrefixManager()
        self.items = set()
        self.errors = []

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
            self.validate_edge(oid, sid, ad)

    def validate_node(self, nid, ad):
        self.validate_id(nid)
        self.validate_props(ad)

    def validate_edge(self, oid, sid, ad):
        self.validate_id(oid)
        self.validate_id(sid)
        self.validate_props(ad)
        
    def validate_id(self, id):
        if ":" in id:
            uri = self.prefix_manager.expand(id)
            if uri is None or uri == id:
                self.report(id, "expansion is identical")
        else:
            if id not in self.prefix_manager.prefixmap:
                self.report(id, "no such short form")

    def validate_props(self, ad):
        for p,v in ad.items():
            self.validate_id(p)

    def report(self, item, info=""):
        if item in self.items:
            return
        msg = "Item: {} Message: {}".format(item, info)
        logging.error(msg)
        self.errors.append(msg)
        self.items.add(item)
