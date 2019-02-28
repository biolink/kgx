import pandas as pd
import numpy as np
import logging
import os
import tarfile
from tempfile import TemporaryFile

from .transformer import Transformer

from typing import Dict, List

import re

header_type_pattern = re.compile('^.*:(STRING|STRING\[.*\])$')
header_list_pattern = re.compile('^.*:STRING\[.*\]$')

def get_delimiter(header:str) -> bool:
    """
    If the header encodes a list then the delimiter will be returned. Otherwise,
    None is returned. If no delimiter is specified then it will default to a
    semicolon.
    """
    if header_list_pattern.match(header):
        _, t = header.rsplit(':', 1)
        delimiter = t[1:-1]
        return delimiter if delimiter != '' else ';'
    else:
        return None

class PandasTransformer(Transformer):
    """
    Implements Transformation from a Pandas DataFrame to a NetworkX graph
    """
    _extention_types = {
        'csv' : ',',
        'tsv' : '\t',
        'txt' : '|'
    }

    def __init__(self, graph=None, *, list_delimiter=';'):
        super(PandasTransformer, self).__init__(graph)
        self.list_delimiter = list_delimiter

    def parse(self, filename: str, input_format='csv', **kwargs):
        """
        Parse a CSV/TSV

        May be either a node file or an edge file
        """
        if 'delimiter' not in kwargs:
            kwargs['delimiter'] = self._extention_types[input_format]
        if filename.endswith('.tar'):
            with tarfile.open(filename) as tar:
                for member in tar.getmembers():
                    f = tar.extractfile(member)
                    df = pd.read_csv(f, comment='#', **kwargs) # type: pd.DataFrame
                    if member.name == 'nodes.csv':
                        self.load_nodes(df)
                    elif member.name == 'edges.csv':
                        self.load_edges(df)
                    else:
                        raise Exception('Tar file contains unrecognized member {}'.format(member.name))
        else:
            df = pd.read_csv(filename, comment='#', **kwargs) # type: pd.DataFrame
            self.load(df)


    def build_kwargs(self, obj:Dict) -> Dict:
        """
        Returns a dictionary that represents the same set of attributes but with
        type encodings stripped from keys and applied to their values. If no
        type encodings are used then the returned dict will be equal to the
        original.

        Example
        -------------
        >>> build_kwargs({
                'name:STRING' : 'a',
                'synonym:STRING[;]' : 'c;d;e',
                'id' : 'xyz',
            })
        {'name' : 'a', 'synonym' : ['c', 'd', 'e'], 'id' : 'xyz'}
        """
        kwargs = {}
        for key, value in obj.items():
            if value is np.nan:
                continue
            if header_type_pattern.match(key):
                if header_list_pattern.match(key):
                    delimiter = get_delimiter(key)
                    key, _ = key.split(':', 1)
                    kwargs[key] = value.split(delimiter)
                else:
                    key, _ = key.split(':', 1)
                    kwargs[key] = value
            else:
                kwargs[key] = value
        return kwargs

    def load(self, df: pd.DataFrame):
        if 'subject' in df or 'subject:STRING' in df:
            self.load_edges(df)
        else:
            self.load_nodes(df)

    def load_nodes(self, df:pd.DataFrame):
        for obj in df.to_dict('record'):
            self.load_node(obj)

    def load_node(self, obj:Dict):
        kwargs = self.build_kwargs(obj)
        n = kwargs['id']
        self.graph.add_node(n, **kwargs)

    def load_edges(self, df: pd.DataFrame):
        for obj in df.to_dict('record'):
            self.load_edge(obj)

    def load_edge(self, obj: Dict):
        kwargs = self.build_kwargs(obj)
        s = kwargs['subject']
        o = kwargs['object']
        self.graph.add_edge(s, o, **kwargs)

    def build_export_row(self, data:dict, header_mappings:dict) -> dict:
        row = {}
        for key, value in data.items():
            if key not in header_mappings:
                header_mappings[key] = type(value)
            p = header_mappings[key] == list
            q = isinstance(value, list)
            if p and q:
                row['{}:STRING[{}]'.format(key, self.list_delimiter)] = self.list_delimiter.join(value)
            elif p and not q:
                row['{}:STRING[{}]'.format(key, self.list_delimiter)] = value
            elif not p and q:
                row['{}:STRING'.format(key)] = self.list_delimiter.join(value)
            elif not p and not q:
                row['{}:STRING'.format(key)] = value
            else:
                raise Exception('Values of {} is both {} and {}, must be either list or string'.format(key, type(key), header_mappings[key]))
        return row

    def export_nodes(self) -> pd.DataFrame:
        header_mappings = {}
        rows = []
        for n, data in self.graph.nodes(data=True):
            row = self.build_export_row(data, header_mappings=header_mappings)
            row['id:STRING'] = n
            rows.append(row)
        df = pd.DataFrame.from_dict(rows)
        return df

    def export_edges(self) -> pd.DataFrame:
        header_mappings = {}
        rows = []
        for o,s,data in self.graph.edges(data=True):
            row = self.build_export_row(data, header_mappings)
            row['subject:STRING'] = s
            row['object:STRING'] = o
            rows.append(row)
        df = pd.DataFrame.from_dict(rows)
        # cols = df.columns.tolist()
        # cols = self.order_cols(cols)
        # df = df[cols]
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
