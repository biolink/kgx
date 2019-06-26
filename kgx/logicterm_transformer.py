import logging
import pandas as pd
import networkx as nx
from prologterms import Term, SExpressionRenderer, PrologRenderer
from kgx.transformer import Transformer

from typing import Dict, List, Union

class LogicTermTransformer(Transformer):
    """
    TODO: Motivation for LogicTermTransformer?
    """

    def __init__(self, source:Union[Transformer, nx.MultiDiGraph]=None, output_format=None, **args):
        super().__init__(source=source, **args)
        if output_format is not None and output_format == 'prolog':
            self.renderer = PrologRenderer()
        else:
            self.renderer = SExpressionRenderer()

    def export_nodes(self) -> pd.DataFrame:
        items = []
        for n,data in self.graph.nodes(data=True):
            for k,v in data.items():
                self.write_term('node_prop', n, k, v)

    def export_edges(self) -> pd.DataFrame:
        items = []
        for o,s,data in self.graph.edges(data=True):
            el = data.get('edge_label', None)
            self.write_term('edge', el, o, s)
            for k,v in data.items():
                self.write_term('edge_prop', el, o, s, k, v)

    def write_term(self, pred: str, *args):
        t = Term(pred, *args)
        self.file.write(self.renderer.render(t) + "\n")


    def save(self, filename: str, format='sxpr', zipmode='w', **kwargs):
        """
        """
        with open(filename,'w') as file:
            self.file = file
            self.export_nodes()
            self.export_edges()
