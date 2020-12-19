import pandas as pd
from prologterms import Term, SExpressionRenderer, PrologRenderer

from kgx.config import get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.transformers.transformer import Transformer

from typing import Dict, List, Union, Optional

log = get_logger()


class LogicTermTransformer(Transformer):
    """
    TODO
    """

    def __init__(self, source_graph: Optional[BaseGraph] = None, output_format: Optional[str] = None, **kwargs: Dict):
        super().__init__(source_graph=source_graph)
        if output_format is not None and output_format == 'prolog':
            self.renderer = PrologRenderer()
        else:
            self.renderer = SExpressionRenderer()

    def export_nodes(self) -> pd.DataFrame:
        items: List = []
        for n,data in self.graph.nodes(data=True):
            for k,v in data.items():
                self.write_term('node_prop', n, k, v)

    def export_edges(self) -> pd.DataFrame:
        items: List = []
        for o,s,data in self.graph.edges(data=True):
            el = data.get('predicate', None)
            self.write_term('edge', el, o, s)
            for k,v in data.items():
                self.write_term('edge_prop', el, o, s, k, v)

    def write_term(self, pred: str, *args):
        t: Term = Term(pred, *args)
        self.file.write(self.renderer.render(t) + "\n")

    def save(self, filename: str, output_format='sxpr', zipmode='w', **kwargs):
        """
        """
        with open(filename,'w') as file:
            self.file = file
            self.export_nodes()
            self.export_edges()
