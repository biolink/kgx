from typing import Dict, Generator, Any, Union, Optional, Tuple, List, Set, Callable
import re

from kgx.utils.kgx_utils import knowledge_provenance_properties, column_types
from kgx.prefix_manager import PrefixManager
from kgx.config import get_logger

log = get_logger()


class Source(object):
    """
    A Source is responsible for reading data as records
    from a store where the store is a file or a database.
    """

    def __init__(self):
        self.graph_metadata: Dict = {}
        self.node_filters = {}
        self.edge_filters = {}
        self.node_properties = set()
        self.edge_properties = set()
        self.prefix_manager = PrefixManager()
        self.default_provenance = 'Graph'
        self._infores_catalog: Dict[str, Set[str]] = dict()

    def set_prefix_map(self, m: Dict) -> None:
        """
        Update default prefix map.

        Parameters
        ----------
        m: Dict
            A dictionary with prefix to IRI mappings

        """
        self.prefix_manager.update_prefix_map(m)

    def _infores_to_catalog(self, infores: str, source: str):
        """
        Catalogs a mapping of InfoRes to a Knowledge Source name.

        Parameters
        ----------
        infores: str
            Information Resource CURIE to be added to the catalog,

        source: str
            Name of Information Resource associated with the InfoRes
            (i.e. from which the InfoRes was inferred)

        """
        if infores not in self._infores_catalog:
            self._infores_catalog[infores] = set()
        self._infores_catalog[infores].add(source)

    def _infores_processor(
            self,
            ksf: str,
            infores_rewrite_filter: Optional[Tuple] = None
    ) -> Callable:
        """
        Full processor of a Knowledge Source name into an InfoRes. The conversion is made based on
        client-caller specfied rewrite rules for a given knowledge source field ('ksf').

        Parameters
        ----------
        ksf: str
            Knowledge source field being processed.

        infores_rewrite_filter: Optional[Tuple]
            The presence of this optional Tuple argument signals an InfoRes rewrite of any
            Biolink 2.0 compliant knowledge source field name in node and edge data records.
            The mere presence of a (possibly empty) Tuple signals a rewrite. If the Tuple is empty,
            then only a standard transformation of the field value is performed. If the Tuple has
            an infores_rewrite[0] value, it is assumed to be a regular expression (string) to match
            against. If there is no infores_rewrite[1] value or it is empty, then matches of the
            infores_rewrite[0] are simply deleted from the field value prior to coercing the field
            value into an InfoRes CURIE. Otherwise, a non-empty second string value of infores_rewrite[1]
            is a substitution string for the regex value matched in the field. If the Tuple contains
            a third non-empty string (as infores_rewrite[2]), then the given string is added as a prefix
            to the InfoRes.  Whatever the transformations, unique InfoRes identifiers once generated,
            are used in the meta_knowledge_graph and also reported using the get_infores_catalog() method.

        Returns
        -------
        Callable
            A locally configured Callable that knows how to process
            a source name string into an infores CURIE, using on client-specified
            rewrite rules applied alongside standard formatting rules.

        """

        # Check for non-empty infores_rewrite_filter
        if infores_rewrite_filter:
            _filter = re.compile(infores_rewrite_filter[0]) if infores_rewrite_filter[0] else None
            _substr = infores_rewrite_filter[1] if len(infores_rewrite_filter) > 1 else ''
            _prefix = infores_rewrite_filter[2] if len(infores_rewrite_filter) > 2 else ''
        else:
            _filter = None
            _substr = ''
            _prefix = ''

        def _process_infores(source: str) -> str:
            """
            Process a single knowledge Source name  string into an infores, by applying rules
            in the _infores_processor() closure context, followed by standard formatting.

            Parameters
            ----------
            source: str
                Knowledge source name string being processed.

            Returns
            -------
            str
                Infores CURIE inferred from the input Knowledge Source name string.

            """
            if _filter:
                infores = _filter.sub(_substr, source)
            else:
                infores = source
            infores = _prefix + ' ' + infores
            infores = infores.strip()
            infores = infores.lower()
            infores = re.sub(r"\s+", "_", infores)
            infores = re.sub(r"[\W]", "", infores)
            infores = re.sub(r"_", "-", infores)

            # TODO: to be fully compliant, the InfoRes needs to have the 'infores' prefix?
            infores = "infores:"+infores

            return infores

        def parser_list(sources: Optional[List[str]] = None) -> List[str]:
            """
            Infores parser for a list of input knowledge source names.

            Parameters
            ----------
            sources: List[str]
                List of Knowledge source name strings being processed.

            Returns
            -------
            List[str]
                Source name strings transformed into infores CURIES, using _process_infores().

            """
            if not sources:
                return [self.default_provenance]
            results:  List[str] = list()
            for source in sources:
                infores: str = _process_infores(source)
                if infores:
                    self._infores_to_catalog(infores, source)
                    results.append(infores)
            return results

        def parser_scalar(source=None) -> str:
            """
            Infores parser for a single knowledge source name string.

            Parameters
            ----------
            source: str
                Knowledge source name string being processed.

            Returns
            -------
            str
                Source name string transformed into an infores CURIE, using _process_infores().

            """
            if not source:
                return self.default_provenance
            infores: str = _process_infores(source)
            if infores:
                self._infores_to_catalog(infores, source)
                return infores
            else:
                return ''

        if ksf in column_types and column_types[ksf] == list:
            return parser_list
        else:
            # not sure how safe an assumption for all non-list column_types, but...
            return parser_scalar

    @staticmethod
    def _infores_default(ksf, default=None):
        """
        Lightweight alternative to _infores_processor which simply assigns provenance fields client-defined
        simple default knowledge source strings (not constrained to be formatted as infores CURIEs).

        Parameters
        ----------
        ksf: str
            Knowledge source field being processed.

        Returns
        -------
        Callable
            A locally configured Callable that knows how to process a source name string
            (possibly empty) into a suitable (possibly default) infores string identifier.

        """

        def default_value_list(sources: List[str] = None):
            """
            Infores default method for a list of input knowledge source names.

            Parameters
            ----------
            sources: List[str]
                List of Knowledge source name strings being processed.

            Returns
            -------
            List[str]
                Infores identifiers mapped to input source strings.

            """
            if not default:
                return list()
            if not sources:
                return [default]
            else:
                return sources

        def default_value_scalar(source=None):
            """
            Infores default method for single input knowledge source name.

            Parameters
            ----------
            source: str
                Knowledge source name string being processed.

            Returns
            -------
            str
                Infores identifier mapped to the input source string.

            """
            if not default:
                return None
            if not source:
                return default
            else:
                return source

        if ksf in column_types and column_types[ksf] == list:
            return default_value_list
        else:
            # not sure how safe an assumption for non-list column_types, but...
            return default_value_scalar

    def set_provenance_map(self, kwargs: Dict):
        """
        A provenance property indexed map set up with various graph_metadata
        Callable methods to process input knowledge source values into
        suitable InfoRes identifiers.

        Parameters
        ----------
        kwargs: Dict
            The input keyword argument dictionary was likely propagated from the
            Transformer.transform() method input_args, and is here harvested for
            static defaults or rewrite rules for provenance slot InfoRes value processing.

        """
        if 'default_provenance' in kwargs:
            self.default_provenance = kwargs.pop('default_provenance')

        # Biolink 2.0 provenance 'knowledge_source' derived fields
        ksf_found = False
        for ksf in knowledge_provenance_properties:
            if ksf in kwargs:
                if not ksf_found:
                    ksf_found = ksf  # save the first one found, for later
                ksf_value = kwargs.pop(ksf)
                if isinstance(ksf_value, str):
                    ksf_value = ksf_value.strip()
                    if ksf_value.lower() == 'true':
                        self.graph_metadata[ksf] = self._infores_processor(ksf)
                    elif ksf_value.lower() == 'false':
                        self.graph_metadata[ksf] = self._infores_default(ksf)  # source suppressed
                    else:
                        self.graph_metadata[ksf] = self._infores_default(ksf, ksf_value)
                elif isinstance(ksf_value, bool):
                    if ksf_value:
                        self.graph_metadata[ksf] = self._infores_processor(ksf)
                    else:  # false, ignore this source?
                        self.graph_metadata[ksf] = self._infores_default(ksf)  # source suppressed
                elif isinstance(ksf_value, (list, set, tuple)):
                    self.graph_metadata[ksf] = self._infores_processor(ksf, infores_rewrite_filter=ksf_value)

        # if none specified, add at least one generic 'knowledge_source'
        if not ksf_found:
            if 'name' in kwargs:
                self.graph_metadata['knowledge_source'] = self._infores_default(ksf, kwargs['name'])
            else:
                self.graph_metadata['knowledge_source'] = self._infores_default(ksf, self.default_provenance)
            ksf_found = 'knowledge_source'  # knowledge source field 'ksf' is set, one way or another

        # TODO: better to lobby the team to totally deprecated this, even for Nodes?
        if 'provided_by' not in self.graph_metadata:
            self.graph_metadata['provided_by'] = self.graph_metadata[ksf_found]

    def set_provenance(self, ksf: str, data: Dict):
        """
            Compute the provenance value for the current node or edge data, using the
            infores rewrite context previously established by a call to set_provenance_map().

            Parameters
            ----------
            ksf: str
                Knowledge source field being processed.
            data: Dict
                Current node or edge data entry being processed.

        """
        if ksf not in data.keys():
            if ksf in self.graph_metadata:
                data[ksf] = self.graph_metadata[ksf]()  # get default ksf value?
            else:
                data[ksf] = [self.default_provenance]
        else:  # valid data value but... possible InfoRes rewrite?
            if isinstance(data[ksf], (list, set, tuple)):
                sources = list(data[ksf])
            else:
                if column_types[ksf] == list:
                    sources = [data[ksf]]
                else:
                    sources = data[ksf]
            if ksf in self.graph_metadata:
                data[ksf] = self.graph_metadata[ksf](sources)
            else:  # leave data intact?
                data[ksf] = sources

        # ignore if again empty at this point
        if not data[ksf]:
            data.pop(ksf)

    def set_node_provenance(self, node_data: Dict):
        """
        Sets the node provenance value for the current node. At the moment, nodes are still
        hard-coded to using the (Biolink 2.0 deprecated) 'provided_by' provenance property;
        However, this could change in the future to use edge 'knowledge_source' properties.

        Parameters
        ----------
        node_data: Dict
            Current node data entry being processed.

        """
        self.set_provenance('provided_by', node_data)

    # TODO: need to design a more efficient algorithm here...
    def set_edge_provenance(self, edge_data: Dict):
        """
        Sets the node provenance value for the current node. Edge provenance properties
        include the full Biolink 2.0 'knowledge_source' related properties.

        Parameters
        ----------
        edge_data: Dict
            Current edge data entry being processed.

        """
        ksf_found = False
        data_fields = list(edge_data.keys())
        for ksf in data_fields:
            if ksf in knowledge_provenance_properties:
                ksf_found = True
                self.set_provenance(ksf, edge_data)
        if not ksf_found:
            for ksf in self.graph_metadata:
                if ksf != 'provided_by':
                    self.set_provenance(ksf, edge_data)

    def get_infores_catalog(self) -> Dict[str, Set[str]]:
        """
        Retrieves the catalogs mapping of Knowledge Source names to an InfoRes.

        Returns
        -------
        Dict[str, Set[str]]
            Dictionary where the index string is an InfoRes CURIE, values are sets of Knowledge Source Names

        """
        return self._infores_catalog

    def parse(self, **kwargs: Any) -> Generator:
        """
        This method reads from the underlying store, using the
        arguments provided in ``config`` and yields records.

        Parameters
        ----------
        **kwargs: Any

        Returns
        -------
        Generator

        """
        pass

    def check_node_filter(self, node: Dict) -> bool:
        """
        Check if a node passes defined node filters.

        Parameters
        ----------
        node: Dict
            A node

        Returns
        -------
        bool
            Whether the given node has passed all defined node filters

        """
        pass_filter = False
        if self.node_filters:
            for k, v in self.node_filters.items():
                if k in node:
                    # filter key exists in node
                    if isinstance(v, (list, set, tuple)):
                        if any(x in node[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if node[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} node filter of type {type(v)}")
                        return False
                else:
                    # filter key does not exist in node
                    return False
        else:
            # no node filters defined
            pass_filter = True
        return pass_filter

    def check_edge_filter(self, edge: Dict) -> bool:
        """
        Check if an edge passes defined edge filters.

        Parameters
        ----------
        edge: Dict
            An edge

        Returns
        -------
        bool
            Whether the given edge has passed all defined edge filters

        """
        pass_filter = False
        if self.edge_filters:
            for k, v in self.edge_filters.items():
                if k in {'subject_category', 'object_category'}:
                    pass_filter = True
                    continue
                if k in edge:
                    # filter key exists in edge
                    if isinstance(v, (list, set, tuple)):
                        if any(x in edge[k] for x in v):
                            pass_filter = True
                        else:
                            return False
                    elif isinstance(v, str):
                        if edge[k] == v:
                            pass_filter = True
                        else:
                            return False
                    else:
                        log.error(f"Unexpected {k} edge filter of type {type(v)}")
                        return False
                else:
                    # filter does not exist in edge
                    return False
        else:
            # no edge filters defined
            pass_filter = True
        return pass_filter

    def set_node_filter(self, key: str, value: Union[str, set]) -> None:
        """
        Set a node filter, as defined by a key and value pair.
        These filters are used to filter (or reduce) the
        search space when fetching nodes from the underlying store.

        .. note::
            When defining the 'category' filter, the value should be of type ``set``.
            This method also sets the 'subject_category' and 'object_category'
            edge filters, to get a consistent set of nodes in the subgraph.

        Parameters
        ----------
        key: str
            The key for node filter
        value: Union[str, set]
            The value for the node filter.
            Can be either a string or a set.

        """
        if key == 'category':
            if isinstance(value, set):
                if 'subject_category' in self.edge_filters:
                    self.edge_filters['subject_category'].update(value)
                else:
                    self.edge_filters['subject_category'] = value
                if 'object_category' in self.edge_filters:
                    self.edge_filters['object_category'].update(value)
                else:
                    self.edge_filters['object_category'] = value
            else:
                raise TypeError("'category' node filter should have a value of type 'set'")

        if key in self.node_filters:
            self.node_filters[key].update(value)
        else:
            self.node_filters[key] = value

    def set_node_filters(self, filters: Dict) -> None:
        """
        Set node filters.

        Parameters
        ----------
        filters: Dict
            Node filters

        """
        if filters:
            for k, v in filters.items():
                if isinstance(v, (list, set, tuple)):
                    self.set_node_filter(k, set(v))
                else:
                    self.set_node_filter(k, v)

    def set_edge_filters(self, filters: Dict) -> None:
        """
        Set edge filters.

        Parameters
        ----------
        filters: Dict
            Edge filters

        """
        if filters:
            for k, v in filters.items():
                if isinstance(v, (list, set, tuple)):
                    self.set_edge_filter(k, set(v))
                else:
                    self.set_edge_filter(k, v)

    def set_edge_filter(self, key: str, value: set) -> None:
        """
        Set an edge filter, as defined by a key and value pair.
        These filters are used to filter (or reduce) the
        search space when fetching nodes from the underlying store.

        .. note::
            When defining the 'subject_category' or 'object_category' filter,
            the value should be of type ``set``.
            This method also sets the 'category' node filter, to get a
            consistent set of nodes in the subgraph.

        Parameters
        ----------
        key: str
            The key for edge filter
        value: Union[str, set]
            The value for the edge filter.
            Can be either a string or a set.

        """
        if key in {'subject_category', 'object_category'}:
            if isinstance(value, set):
                if 'category' in self.node_filters:
                    self.node_filters['category'].update(value)
                else:
                    self.node_filters['category'] = value
            else:
                raise TypeError(f"'{key}' edge filter should have a value of type 'set'")

        if key in self.edge_filters:
            self.edge_filters[key].update(value)
        else:
            self.edge_filters[key] = value

    def clear_graph_metadata(self):
        """
        Clears a Source graph's internal graph_metadata. The value of such graph metadata is (now)
        generally a Callable function. This operation can be used in the code when the metadata is
        no longer needed, but may cause peculiar Python object persistent problems downstream.
        """
        self.graph_metadata.clear()
