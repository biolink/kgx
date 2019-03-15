import pandas as pd
import numpy as np
import logging
import os
import tarfile
from tempfile import TemporaryFile

from kgx.utils import make_path

from .transformer import Transformer

from typing import Dict, List, Optional

LIST_DELIMITER = '|'

_column_types = {
    'publications' : list,
    'qualifiers' : list,
    'category' : list,
    'synonym' : list,
    'provided_by' : list,
    'same_as' : list,
    'negated' : bool,
}

class PandasTransformer(Transformer):
    """
    Implements Transformation from a Pandas DataFrame to a NetworkX graph
    """
    _extention_types = {
        'csv' : ',',
        'tsv' : '\t',
        'txt' : '|'
    }

    def parse(self, filename: str, input_format='csv', **kwargs):
        """
        Parse a CSV/TSV

        May be either a node file or an edge file
        """
        self.filename = filename
        if 'delimiter' not in kwargs:
            kwargs['delimiter'] = self._extention_types[input_format]
        if filename.endswith('.tar'):
            with tarfile.open(filename) as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    df = pd.read_csv(f, comment='#', dtype=str, **kwargs) # type: pd.DataFrame
                    if member.name == 'nodes.csv':
                        self.load_nodes(df)
                    elif member.name == 'edges.csv':
                        self.load_edges(df)
                    else:
                        raise Exception('Tar file contains unrecognized member {}'.format(member.name))
        else:
            df = pd.read_csv(filename, comment='#', dtype=str, **kwargs) # type: pd.DataFrame
            self.load(df)

    def load(self, df: pd.DataFrame):
        if 'subject' in df:
            self.load_edges(df)
        else:
            self.load_nodes(df)

    def build_kwargs(self, data:dict) -> dict:
        data = {k : v for k, v in data.items() if v is not np.nan}
        for key, value in data.items():
            if key in _column_types:
                if _column_types[key] == list:
                    if isinstance(value, (list, set, tuple)):
                        data[key] = list(value)
                    elif isinstance(value, str):
                        data[key] = value.split(LIST_DELIMITER)
                    else:
                        data[key] = [str(value)]
                elif _column_types[key] == bool:
                    try:
                        data[key] = bool(value)
                    except:
                        data[key] = False
                else:
                    data[key] = str(value)
        return data

    def load_nodes(self, df:pd.DataFrame):
        for obj in df.to_dict('record'):
            self.load_node(obj)

    def load_node(self, obj:Dict):
        kwargs = self.build_kwargs(obj.copy())
        
        if 'id' not in kwargs:
            print('node in {} with missing id: {}'.format(self.filename, obj))
            return
        
        n = kwargs['id']
        self.graph.add_node(n, **kwargs)

    def load_edges(self, df: pd.DataFrame):
        for obj in df.to_dict('record'):
            self.load_edge(obj)

    def load_edge(self, obj: Dict):
        kwargs = self.build_kwargs(obj.copy())
        if 'subject' not in kwargs:
            print('node in {} with missing subject: {}'.format(self.filename, obj))
            return
        if 'object' not in kwargs:
            print('node in {} with missing object: {}'.format(self.filename, obj))
            return
        s = kwargs['subject']
        o = kwargs['object']
        self.graph.add_edge(s, o, **kwargs)

    def build_export_row(self, data:dict) -> dict:
        """
        Casts all values to primitive types like str or bool according to the
        specified type in `_column_types`. Lists become pipe delimited strings.
        """
        data = {k : v for k, v in data.items() if v is not np.nan}
        for key, value in data.items():
            if key in _column_types:
                if _column_types[key] == list:
                    if isinstance(value, (list, set, tuple)):
                        data[key] = LIST_DELIMITER.join(value)
                    else:
                        data[key] = str(value)
                elif _column_types[key] == bool:
                    try:
                        data[key] = bool(value)
                    except:
                        data[key] = False
                else:
                    data[key] = str(value)
        return data


    def export_nodes(self, encode_header_types=False) -> pd.DataFrame:
        rows = []
        for n, data in self.graph.nodes(data=True):
            row = self.build_export_row(data.copy())
            row['id'] = n
            rows.append(row)
        df = pd.DataFrame.from_dict(rows)
        return df

    def export_edges(self, encode_header_types=False) -> pd.DataFrame:
        rows = []
        for s, o, data in self.graph.edges(data=True):
            row = self.build_export_row(data.copy())
            row['subject'] = s
            row['object'] = o
            rows.append(row)
        df = pd.DataFrame.from_dict(rows)
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

        make_path(filename)
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
        df.to_csv(filename, index=False)
