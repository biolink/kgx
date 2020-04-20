import json
from .sink import Sink

class FileSink(Sink):
    def __init__(self, file_name_prefix, extension):
        self.edge_count = 0
        self.fn_prefix = file_name_prefix
        self.fn_ext = extension

    def _writer(self, out, fields):
        raise NotImplementedError('writer')

    def _open(self, name):
        return open(self.fn_prefix + '.' + name + '.' + self.fn_ext, 'w')

    def add_node(self, node_id, attributes):
        node = {}
        node[':ID'] = node_id
        self.node_writer.writerow(node)
        # TODO: process attributes as nodeprops?
        for k,v in attributes.items():
            prop = {':ID': node_id, 'propname': k, 'value': json.dumps(v)}
            self.nodeprop_writer.writerow(prop)

    def add_edge(self, subject_id, object_id, attributes):
        # TODO: should we generate our own arbitrary :IDs in this way?
        # Can we use a canonical id property instead?
        eid = self.edge_count
        edge = {}
        edge[':START'] = subject_id
        edge[':END'] = object_id
        edge[':ID'] = eid
        self.edge_writer.writerow(edge)
        # TODO: process attributes as edgeprops?
        for k,v in attributes.items():
            prop = {':ID': eid, 'propname': k, 'value': json.dumps(v)}
            self.edgeprop_writer.writerow(prop)
        self.edge_count += 1

    def __enter__(self):
        self.node_out = self._open('node')
        self.edge_out = self._open('edge')
        # TODO: are these node keys and edge keys appropriate?
        # https://github.com/NCATS-Tangerine/kgx/blob/master/Loading.md
        # Is this information accurate?  RTX properties don't seem consistent
        # with this documentation.
        self.node_writer = self._writer(self.node_out, (':ID',
                                                      # TODO: where do we find category:LABEL?
                                                      # RTX nodes have a list of labels.
                                                      #'category:LABEL'
                                                      ))
        self.edge_writer = self._writer(self.edge_out, (':ID', ':START', ':END',
                                                      # TODO: where do we find :TYPE?
                                                      # Is this 'predicate'?
                                                      #':TYPE',
                                                      ))
        self.node_writer.writeheader()
        self.edge_writer.writeheader()
        # TODO:
        self.nodeprop_out = self._open('nodeprop')
        self.edgeprop_out = self._open('edgeprop')
        self.nodeprop_writer = self._writer(self.nodeprop_out, (':ID', 'propname', 'value'))
        self.edgeprop_writer = self._writer(self.edgeprop_out, (':ID', 'propname', 'value'))
        self.nodeprop_writer.writeheader()
        self.edgeprop_writer.writeheader()
        return self

    def __exit__(self, *args, **kwargs):
        self.node_out.close()
        self.edge_out.close()
        # TODO:
        self.nodeprop_out.close()
        self.edgeprop_out.close()
