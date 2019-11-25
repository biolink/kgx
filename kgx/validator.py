import re

import click
import requests
import validators

from kgx.utils.kgx_utils import get_toolkit
from .prefix_manager import PrefixManager

BIOLINK_MODEL = 'https://biolink.github.io/biolink-model/biolink-model.yaml'
CONTEXT_JSONLD = 'https://biolink.github.io/biolink-model/context.jsonld'


class Error(object):
    def __init__(self, error_type, message=None):
        self.error_type = error_type
        self.message = message if message is not None else error_type


class NodeError(Error):
    def __init__(self, node, error_type, message=None):
        super().__init__(error_type, message)
        self.node = node


class EdgeError(Error):
    def __init__(self, subject, object, error_type, message=None):
        super().__init__(error_type, message)
        self.subject = subject
        self.object = object


def is_curie(s:str) -> bool:
    return re.match(r'^[^ :]+:[^ :]+$', s)


class Validator(object):
    """
    Object for validating a property graph
    """

    def __init__(self):
        self.toolkit = get_toolkit()
        self.prefix_manager = PrefixManager()
        self.errors = []

        try:
            self.jsonld = requests.get(CONTEXT_JSONLD).json()
        except:
            raise Exception('Unable to download jsonld file from {}'.format(CONTEXT_JSONLD))

    def ok(self):
        return len(self.errors) == 0

    def validate(self, G):
        """
        Validate a property graph

        Test all node and edge properties plus relationship types are declared
        """
        self.validate_categories(G)
        self.validate_edge_labels(G)

        self.validate_required_node_properties(G)
        self.validate_node_property_types(G)
        self.validate_node_property_values(G)

        self.validate_required_edge_properties(G)
        self.validate_edge_property_types(G)
        self.validate_edge_property_values(G)

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

    def validate_categories(self, G):
        with click.progressbar(G.nodes(data=True), label='validating category for nodes') as bar:
            for n, data in bar:
                categories = data.get('category')
                if categories is None:
                    self.log_node_error(n, 'absent category')
                elif not isinstance(categories, list):
                    self.log_node_error(n, 'invalid category type', message='category type is {} when it should be {}'.format(type(categories), list))
                else:
                    for category in categories:
                        if not self.toolkit.is_category(category):
                            self.log_node_error(n, 'invalid category', message='{} not in biolink model'.format(category))
                        else:
                            c = self.toolkit.get_element(category)
                            if category != c.name and category in c.aliases:
                                self.log_node_error(n, 'alias category', message='should not use alias {} for {}'.format(c.name, category))

    def validate_edge_labels(self, G):
        TYPE = 'invalid edge label'
        with click.progressbar(G.edges(data=True), label='validating edge_label for edges') as bar:
            for u, v, data in bar:
                edge_label = data.get('edge_label')
                if edge_label is None:
                    self.log_edge_error(u, v, 'absent edge label')
                elif not isinstance(edge_label, str):
                    self.log_edge_error(u, v, TYPE, message='edge label type is {} when it should be {}'.format(type(edge_label), str))
                else:
                    p = self.toolkit.get_element(edge_label)
                    if p is None:
                        self.log_edge_error(u, v, TYPE, message='{} not in biolink model'.format(edge_label))
                    elif edge_label != p.name and edge_label in p.aliases:
                        self.log_edge_error(u, v, TYPE, message='should not use alias {} for {}'.format(p.name, edge_label))
                    elif not re.match(r'^[a-z_]*$', edge_label):
                        self.log_edge_error(u, v, TYPE, message='"{}" is not snake case'.format(edge_label))

    def validate_required_node_properties(self, G):
        """
        Checks that if a property is required then it is present
        """
        TYPE='invalid node property'
        node_properties = self.toolkit.children('node property')
        required_properties = []

        for p in node_properties:
            e = self.toolkit.get_element(p)
            if hasattr(e, 'required') and e.required:
                required_properties.append(e.name)

        if 'id' in required_properties:
            required_properties.remove('id')

        if required_properties == []:
            return

        with click.progressbar(G.nodes(data=True), label='validate that required node properties are present') as bar:
            for n, data in bar:
                for p in required_properties:
                    if p not in data:
                        self.log_node_error(n, TYPE, message='missing required property "{}"'.format(p))

    def validate_required_edge_properties(self, G):
        """
        Checks that if a property is required then it is present
        """
        edge_properties = self.toolkit.children('association slot')
        required_properties = []

        for p in edge_properties:
            e = self.toolkit.get_element(p)
            if hasattr(e, 'required') and e.required:
                required_properties.append(e.name)

        if 'subject' in required_properties:
            required_properties.remove('subject')

        if 'object' in required_properties:
            required_properties.remove('object')

        if required_properties == []:
            return

        with click.progressbar(G.edges(data=True), label='validate that required node properties are present') as bar:
            for u, v, data in bar:
                for p in required_properties:
                    if p not in data:
                        self.log_edge_error(u, v, 'absent node property', message='missing required property "{}"'.format(p))

    def validate_node_property_values(self, G):
        TYPE='invalid node property'
        prefixes = set(key for key, value in self.jsonld['@context'].items() if isinstance(value, str))
        with click.progressbar(G.nodes(data=True), label='validate node property values') as bar:
            for n, data in bar:
                if not re.match(r'^[^ :]+:[^ :]+$', n):
                    self.log_node_error(n, TYPE, message='identifier "{}" does not have curie syntax'.format(n))
                else:
                    prefix, _ = n.split(':')
                    if prefix not in prefixes:
                        self.log_node_error(n, TYPE, message='prefix "{}" is not in jsonld: https://biolink.github.io/biolink-model/context.jsonld'.format(prefix))

    def validate_edge_property_values(self, G):
        TYPE='invalid edge property'
        prefixes = set(key for key, value in self.jsonld['@context'].items() if isinstance(value, str))
        with click.progressbar(G.edges(data=True), label='validate node property values') as bar:
            for s, o, data in bar:
                if not is_curie(s):
                    self.log_edge_error(s, o, TYPE, message='subject "{}" does not have curie syntax'.format(s))
                else:
                    prefix, _ = s.split(':')
                    if prefix not in prefixes:
                        self.log_edge_error(s, o, TYPE, message='prefix "{}" is not in jsonld: https://biolink.github.io/biolink-model/context.jsonld'.format(prefix))

                if not is_curie(o):
                    self.log_edge_error(o, TYPE, message='object "{}" does not have curie syntax'.format(o))
                else:
                    prefix, _ = s.split(':')
                    if prefix not in prefixes:
                        self.log_edge_error(s, o, TYPE, message='prefix "{}" is not in jsonld: https://biolink.github.io/biolink-model/context.jsonld'.format(prefix))

    def validate_node_property_types(self, G):
        TYPE = 'invalid node property'
        with click.progressbar(G.nodes(data=True), label='validate node property types') as bar:
            for n, data in bar:
                if not isinstance(n, str):
                    self.log_node_error(n, TYPE, message='expect type of id to be str, instead got {}'.format(type(n)))

                for key, value in data.items():
                    e = self.toolkit.get_element(key)
                    if hasattr(e, 'typeof'):
                        if e.typeof == 'string' and not isinstance(value, str):
                            self.log_node_error(n, TYPE, message='expected type of {} to be str, instead got {}'.format(key, type(value)))
                        elif e.typeof == 'uri' and not isinstance(value, str) and not validators.url(value):
                            self.log_node_error(n, TYPE, message='value for param {} is not a uri: {}'.format(key, value))
                        elif e.typeof == 'double' and not isinstance(value, (int, float)):
                            self.log_node_error(n, TYPE, message='expected type of {} to be float, instead got {}'.format(key, type(value)))
                        elif e.typeof == 'time':
                            # Don't know how to test this yet
                            pass
                    if hasattr(e, 'multivalued'):
                        if e.multivalued:
                            if not isinstance(value, list):
                                self.log_node_error(n, TYPE, message='expected type of {} to be list, instead got {}'.format(key, type(value)))
                        else:
                            if isinstance(value, (list, set, tuple)):
                                self.log_node_error(n, TYPE, message='{} is not multivalued but was type {}'.format(key, type(value)))

    def validate_edge_property_types(self, G):
        TYPE = 'invalid edge property'
        with click.progressbar(G.edges(data=True), label='validate edge property types') as bar:
            for s, o, data in bar:
                if not isinstance(s, str):
                    self.log_edge_error(s, o, TYPE, message='expect type of subject to be str, instead got {}'.format(type(s)))
                if not isinstance(o, str):
                    self.log_edge_error(s, o, TYPE, message='expect type of subject to be str, instead got {}'.format(type(o)))

                for key, value in data.items():
                    e = self.toolkit.get_element(key)
                    if hasattr(e, 'typeof'):
                        if (e.typeof == 'string' or e.typeof == 'uri') and not isinstance(value, str):
                            self.log_edge_error(s, o, TYPE, message='expected type of {} to be str, instead got {}'.format(key, type(value)))
                        elif e.typeof == 'uri' and not isinstance(value, str) and not validators.url(value):
                            self.log_node_error(n, TYPE, message='value for param {} is not a uri: {}'.format(key, value))
                        elif e.typeof == 'double' and not isinstance(value, (int, float)):
                            self.log_edge_error(s, o, TYPE, message='expected type of {} to be float, instead got {}'.format(key, type(value)))
                        elif e.typeof == 'time':
                            # Don't know how to test this yet
                            pass
                    if hasattr(e, 'multivalued'):
                        if e.multivalued:
                            if not isinstance(value, list):
                                self.log_edge_error(s, o, TYPE, message='expected type of {} to be list, instead got {}'.format(key, type(value)))
                        else:
                            if isinstance(value, (list, set, tuple)):
                                self.log_edge_error(s, o, TYPE, message='{} is not multivalued but was type {}'.format(key, type(value)))

    def log_edge_error(self, u, v, error_type=None, *, message=None):
        self.errors.append(EdgeError(u, v, error_type, message))

    def log_node_error(self, n, error_type=None, *, message=None):
        self.errors.append(NodeError(n, error_type, message))
