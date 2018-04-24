import pandas as pd
from transformer import Transformer

class PandasTransformer(Transformer):
    """
    Implements Transformation from a Pandas DataFrame to a NetworkX graph
    """
    
    def parse(self, fn, **args):
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
        for _,row in df.iterrows():
            self.load_node(row)
            
    def load_node(self, obj):
        id = obj['id']
        self.graph.add_node(id, attr_dict=obj)
            
    def load_edges(self, df):
        for _,row in df.iterrows():
            self.load_edge(row)

    def load_edge(self, obj):
        s = obj['subject']
        o = obj['object']
        self.graph.add_node(o, s, attr_dict=obj)
            
