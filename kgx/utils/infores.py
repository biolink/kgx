"""
Biolink 2.0 Information Resource (InfoRes) utilities
"""
import re
from typing import Optional, Tuple, Callable, Dict, List, Any

from kgx.utils.kgx_utils import knowledge_provenance_properties, column_types


class InfoResContext:
    """
    Information Resource CURIE management context for knowledge sources.
    """

    def __init__(self):

        self.default_provenance = "Graph"

        # this dictionary captures the operational mappings
        # for a specified knowledge source field in the graph
        self.mapping: Dict[str, Any] = dict()

        # this dictionary records specific knowledge source
        # name to infores associations for the given graph
        self.catalog: Dict[str, str] = dict()

    def get_catalog(self) -> Dict[str, str]:
        """
        Retrieves the catalog of mappings of Knowledge Source names to an InfoRes.

        Returns
        -------
        Dict[str, str]
            Dictionary where the index string is Knowledge Source Names and values are the corresponding InfoRes CURIE

        """
        return self.catalog

    class InfoResMapping:
        """
        Knowledge Source mapping onto an Information Resource identifier.
        """

        def __init__(self, context, ksf: str):
            """
            InfoRes mapping specification for a single knowledge_source (or related) field

            Parameters
            ----------
            context: InfoResContext
                The KGX knowledge graph and default configuration context within which this InfoResMapping exists.
            ksf: str
                Knowledge Source Field being processed.

            """
            self.context = context  # parent InfoRes context
            self.ksf = ksf  # Biolink 2.* 'Knowledge Source Field' slot name
            self.filter = None
            self.substr = ""
            self.prefix = ""

        def processor(self, infores_rewrite_filter: Optional[Tuple] = None) -> Callable:
            """
            Full processor of a Knowledge Source name into an InfoRes. The conversion is made based on
            client-caller specified rewrite rules for a given knowledge source field ('ksf').

            Parameters
            ----------
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
                self.filter = (
                    re.compile(infores_rewrite_filter[0])
                    if infores_rewrite_filter[0]
                    else None
                )
                self.substr = (
                    infores_rewrite_filter[1] if len(infores_rewrite_filter) > 1 else ""
                )
                self.prefix = (
                    infores_rewrite_filter[2] if len(infores_rewrite_filter) > 2 else ""
                )

            def _get_infores(source: str) -> str:
                """
                Get InfoRes CURIE inferred from source name.

                Parameters
                ----------
                source: str
                    Name of Information Resource associated with the InfoRes
                    (i.e. from which the InfoRes was inferred)

                Returns
                -------
                str:
                    infores CURIE, retrieved or generated.

                """
                if source in self.context.catalog:
                    return self.context.catalog[source]
                else:
                    infores: str = _process_infores(source)
                    if infores:
                        self.context.catalog[source] = infores
                        return infores
                    else:
                        return ""

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
                # don't touch something that already looks like an infores CURIE
                if source.startswith("infores:"):
                    return source

                if self.filter:
                    infores = self.filter.sub(self.substr, source)
                else:
                    infores = source
                infores = self.prefix + " " + infores
                infores = infores.strip()
                infores = infores.lower()
                infores = re.sub(r"\s+", "_", infores)
                infores = re.sub(r"\.+", "_", infores)
                infores = re.sub(r"[\W]", "", infores)
                infores = re.sub(r"_", "-", infores)

                # TODO: to be fully compliant, the InfoRes needs to have the 'infores' prefix?
                infores = "infores:" + infores

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
                    return [self.context.default_provenance]
                results: List[str] = list()
                for source in sources:
                    infores = _get_infores(source)
                    if infores:
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
                return (
                    self.context.default_provenance
                    if not source
                    else _get_infores(source)
                )

            if self.ksf in column_types and column_types[self.ksf] == list:
                return parser_list
            else:
                # not sure how safe an assumption for all non-list column_types, but...
                return parser_scalar

        def default(self, default=None):
            """
            Lightweight alternative to the KS processor() which simply assigns knowledge_source fields
            simple client-defined default knowledge source strings (not constrained to be formatted as infores CURIEs).

            Parameters
            ----------
            default: str
                (Optional) default value of the knowledge source field.

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

            if self.ksf in column_types and column_types[self.ksf] == list:
                return default_value_list
            else:
                # not sure how safe an assumption for non-list column_types, but...
                return default_value_scalar

        def set_provenance_map_entry(self, ksf_value: Any) -> Any:
            """
            Set up a provenance (Knowledge Source to InfoRes) map entry
            """
            if isinstance(ksf_value, str):
                ksf_value = ksf_value.strip()
                if ksf_value.lower() == "true":
                    mapping = self.processor()
                elif ksf_value.lower() == "false":
                    mapping = self.default()  # source suppressed
                else:
                    mapping = self.default(ksf_value)
            elif isinstance(ksf_value, bool):
                if ksf_value:
                    mapping = self.processor()
                else:  # false, ignore this source?
                    mapping = self.default()  # source suppressed
            elif isinstance(ksf_value, (list, set, tuple)):
                mapping = self.processor(infores_rewrite_filter=ksf_value)
            else:
                # Not sure what to do here... just return the original ksf_value?
                mapping = ksf_value
            return mapping

    def get_mapping(self, ksf: str) -> InfoResMapping:
        """
        InfoRes mapping for a specified knolwedge source field ('ksf').

        Parameters
        ----------
        ksf: str
            Knowledge Source Field whose mapping is being managed.

        """
        irm = self.InfoResMapping(self, ksf)
        return irm

    def set_provenance_map(self, kwargs: Dict):
        """
        A knowledge_source property indexed map set up with various mapping
        Callable methods to process input knowledge source values into
        suitable InfoRes identifiers.

        Parameters
        ----------
        kwargs: Dict
            The input keyword argument dictionary was likely propagated from the
            Transformer.transform() method input_args, and is here harvested for
            static defaults or rewrite rules for knowledge_source slot InfoRes value processing.

        """
        if "default_provenance" in kwargs:
            self.default_provenance = kwargs.pop("default_provenance")

        # Biolink 2.0 knowledge_source 'knowledge_source' derived fields
        ksf_found = False
        for ksf in knowledge_provenance_properties:
            if ksf in kwargs:
                if not ksf_found:
                    ksf_found = ksf  # save the first one found, for later
                ksf_value = kwargs.pop(ksf)
                # Check if the ksf_value is a multi-valued catalog of patterns for a
                # given knowledge graph field, indexed on each distinct regex pattern
                if isinstance(ksf_value, dict):
                    for ksf_pattern in ksf_value.keys():
                        if ksf not in self.mapping:
                            self.mapping[ksf] = dict()
                        ir = self.get_mapping(ksf)
                        self.mapping[ksf][ksf_pattern] = ir.set_provenance_map_entry(
                            ksf_value[ksf_pattern]
                        )
                else:
                    ir = self.get_mapping(ksf)
                    self.mapping[ksf] = ir.set_provenance_map_entry(ksf_value)

        # if none specified, add at least one generic 'knowledge_source'
        if not ksf_found:
            ksf_found = "knowledge_source"  # knowledge source field 'ksf' is set, one way or another
            ir = self.get_mapping(ksf_found)
            if "name" in kwargs:
                self.mapping["knowledge_source"] = ir.default(kwargs["name"])
            else:
                self.mapping["knowledge_source"] = ir.default(self.default_provenance)

        # TODO: better to lobby the team to totally deprecated this, even for Nodes?
        if "provided_by" not in self.mapping:
            self.mapping["provided_by"] = self.mapping[ksf_found]

    def set_provenance(self, ksf: str, data: Dict):
        """
        Compute the knowledge_source value for the current node or edge data, using the
        infores rewrite context previously established by a call to set_provenance_map().

        Parameters
        ----------
        ksf: str
            Knowledge source field being processed.
        data: Dict
            Current node or edge data entry being processed.

        """
        if ksf not in data.keys():
            if ksf in self.mapping and not isinstance(self.mapping[ksf], dict):
                data[ksf] = self.mapping[ksf]()  # get default ksf value?
            else:
                # if unknown ksf or is an inapplicable pattern
                # dictionary, then just set the value to the default
                data[ksf] = [self.default_provenance]
        else:  # valid data value but... possible InfoRes rewrite?
            # If data is s a non-string iterable
            # then, coerce into a simple list of sources
            if isinstance(data[ksf], (list, set, tuple)):
                sources = list(data[ksf])
            else:
                # Otherwise, just assumed to be a scalar
                # data value for this knowledge source field?
                # Treat differentially depending on column type...
                if column_types[ksf] == list:
                    sources = [data[ksf]]
                else:
                    sources = data[ksf]
            if ksf in self.mapping:
                if isinstance(self.mapping[ksf], dict):
                    # Need to iterate through a knowledge source pattern dictionary
                    for pattern in self.mapping[ksf].keys():
                        for source in sources:
                            # TODO: I need to test pattern match to each source?
                            if re.compile(pattern).match(source):
                                data[ksf] = self.mapping[ksf][pattern]([source])
                            if data[ksf]:
                                break
                else:
                    data[ksf] = self.mapping[ksf](sources)
            else:  # leave data intact?
                data[ksf] = sources

        # ignore if still empty at this point
        if not data[ksf]:
            data.pop(ksf)

    def set_node_provenance(self, node_data: Dict):
        """
        Sets the node knowledge_source value for the current node. At the moment, nodes are still
        hard-coded to using the (Biolink 2.0 deprecated) 'provided_by' knowledge_source property;
        However, this could change in the future to use edge 'knowledge_source' properties.

        Parameters
        ----------
        node_data: Dict
            Current node data entry being processed.

        """
        self.set_provenance("provided_by", node_data)

    # TODO: need to design a more efficient algorithm here...
    def set_edge_provenance(self, edge_data: Dict):
        """
        Sets the node knowledge_source value for the current node. Edge knowledge_source properties
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
            for ksf in self.mapping:
                if ksf != "provided_by":
                    self.set_provenance(ksf, edge_data)
