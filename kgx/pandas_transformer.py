import pandas as pd
from .transformer import Transformer

class PandasTransformer(Transformer):
    """
    Implements Transformation from a Pandas DataFrame to a NetworkX graph
    """
    
    def parse(self, filename, **args):
        """
        Parse a CSV/TSV

        May be either a node file or an edge file
        """
        df = pd.read_csv(filename, comment='#', **args)
        self.load(df)
        
    def load(self, df):
        if 'subject' in df:
            self.load_edges(df)
        else:
            self.load_nodes(df)
            
    def load_nodes(self, df):
        for obj in df.to_dict('record'):
            self.load_node(obj)
            
    def load_node(self, obj):
        id = obj['id']
        self.graph.add_node(id, attr_dict=obj)
            
    def load_edges(self, df):
        for obj in df.to_dict('record'):
            self.load_edge(obj)

    def load_edge(self, obj):
        s = obj['subject']
        o = obj['object']
        self.graph.add_edge(o, s, attr_dict=obj)
            
