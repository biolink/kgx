import pandas as pd
import logging
import os
import tarfile
from tempfile import TemporaryFile

from .transformer import Transformer

from typing import Dict, List

class PandasTransformer(Transformer):
    """
    Implements Transformation from a Pandas DataFrame to a NetworkX graph
    """
    _extention_types = {
        'csv' : ',',
        'tsv' : '\t',
        'txt' : '|'
    }

    def parse(self, filename: str, input_format='csv', **args):
        """
        Parse a CSV/TSV

        May be either a node file or an edge file
        """
        args['delimiter'] = self._extention_types[input_format]
        df = pd.read_csv(filename, comment='#', **args) # type: pd.DataFrame
        self.load(df)

    def load(self, df: pd.DataFrame):
        if 'subject' in df:
            self.load_edges(df)
        else:
            self.load_nodes(df)

    def load_nodes(self, df: pd.DataFrame):
        for obj in df.to_dict('record'):
            self.load_node(obj)

    def load_node(self, obj: Dict):
        id = obj['id'] # type: str
        self.graph.add_node(id, **obj)

    def load_edges(self, df: pd.DataFrame):
        for obj in df.to_dict('record'):
            self.load_edge(obj)

    def load_edge(self, obj: Dict):
        s = obj['subject'] # type: str
        o = obj['object'] # type: str
        self.graph.add_edge(o, s, **obj)

    def export_nodes(self) -> pd.DataFrame:
        items = []
        for n,data in self.graph.nodes_iter(data=True):
            item = data.copy()
            item['id'] = n
            items.append(item)
        df = pd.DataFrame.from_dict(items)
        return df

    def export_edges(self) -> pd.DataFrame:
        items = []
        for o,s,data in self.graph.edges_iter(data=True):
            item = data.copy()
            item['subject'] = s
            item['object'] = o
            items.append(item)
        df = pd.DataFrame.from_dict(items)
        cols = df.columns.tolist()
        cols = self.order_cols(cols)
        df = df[cols]
        return df

    def order_cols(self, cols: List[str]):
        ORDER = ['id', 'subject', 'predicate', 'object', 'relation']
        cols2 = []
        for c in ORDER:
            if c in cols:
                cols2.append(c)
                cols.remove(c)
        return cols2 + cols

    def save(self, filename: str, extention='csv', zipmode='w', **kwargs):
        """
        Write two CSV/TSV files representing the node set and edge set of a
        graph, and zip them in a .tar file.
        """
        if extention not in self._extention_types:
            raise Exception('Unsupported extention: ' + extention)

        if not filename.endswith('.tar'):
            filename += '.tar'

        delimiter = self._extention_types[extention]

        nodes_content = self.export_nodes().to_csv(sep=delimiter, index=False)
        edges_content = self.export_edges().to_csv(sep=delimiter, index=False)

        nodes_file_name = 'nodes.' + extention
        edges_file_name = 'edges.' + extention

        def add_to_tar(tar, filename, filecontent):
            content = filecontent.encode()
            with TemporaryFile() as tmp:
                tmp.write(content)
                tmp.seek(0)
                info = tarfile.TarInfo(name=filename)
                info.size = len(content)
                tar.addfile(tarinfo=info, fileobj=tmp)

        with tarfile.open(name=filename, mode=zipmode) as tar:
            add_to_tar(tar, nodes_file_name, nodes_content)
            add_to_tar(tar, edges_file_name, edges_content)

        return filename

    def save_csv(self, filename: str, type='n', **args):
        """
        Write a CSV/TSV

        May be either a node file or an edge file
        """
        if type == 'n':
            df = self.export_nodes()
        else:
            df = self.export_edges()
        # TODO: order
        df.to_csv(filename, index=False)
