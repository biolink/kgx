"""
KGX Validator class
"""
import re
from typing import List, Optional, Dict, Set, Callable

import click
import validators
from bmt import Toolkit

from kgx.error_detection import ErrorType, MessageLevel, ErrorDetecting
from kgx.config import get_jsonld_context, get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.utils.kgx_utils import (
    get_toolkit,
    snakecase_to_sentencecase,
    sentencecase_to_snakecase,
    camelcase_to_sentencecase,
    GraphEntityType,
)
from kgx.prefix_manager import PrefixManager

logger = get_logger()


class Validator(ErrorDetecting):
    """
    Class for validating a property graph.

    The optional 'progress_monitor' for the validator should be a lightweight Callable
    which is injected into the class 'inspector' Callable, designed to intercepts
    node and edge records streaming through the Validator (inside a Transformer.process() call.
    The first (GraphEntityType) argument of the Callable tags the record as a NODE or an EDGE.
    The second argument given to the Callable is the current record itself.
    This Callable is strictly meant to be procedural and should *not* mutate the record.
    The intent of this Callable is to provide a hook to KGX applications wanting the
    namesake function of passively monitoring the graph data stream. As such, the Callable
    could simply tally up the number of times it is called with a NODE or an EDGE, then
    provide a suitable (quick!) report of that count back to the KGX application. The
    Callable (function/callable class) should not modify the record and should be of low
    complexity, so as not to introduce a large computational overhead to validation!

    Parameters
    ----------
    verbose: bool
        Whether the generated report should be verbose or not (default: ``False``)
    progress_monitor: Optional[Callable[[GraphEntityType, List], None]]
        Function given a peek at the current record being processed by the class wrapped Callable.
    schema: Optional[str]
        URL to (Biolink) Model Schema to be used for validated (default: None, use default Biolink Model Toolkit schema)
    error_log: str
        Where to write any graph processing error message (stderr, by default)
    """

    _the_validator = None

    @classmethod
    def get_the_validator(
            cls,
            verbose: bool = False,
            progress_monitor: Optional[Callable[[GraphEntityType, List], None]] = None,
            schema: Optional[str] = None,
            error_log: str = None
    ):
        """
        Creates and manages a default singleton Validator in the module, when called
        """
        if not cls._the_validator:
            cls.set_biolink_model("v4.1.6")
            cls._the_validator = Validator(
                verbose=verbose,
                progress_monitor=progress_monitor,
                schema=schema,
                error_log=error_log
            )
        return cls._the_validator

    def __init__(
            self,
            verbose: bool = False,
            progress_monitor: Optional[Callable[[GraphEntityType, List], None]] = None,
            schema: Optional[str] = None,
            error_log: str = None
    ):
        ErrorDetecting.__init__(self, error_log)

        # formal arguments
        self.verbose: bool = verbose
        self.progress_monitor: Optional[
            Callable[[GraphEntityType, List], None]
        ] = progress_monitor

        self.schema: Optional[str] = schema

        # internal attributes
        # associated currently active _currently_active_toolkit with this Validator instance
        self.validating_toolkit = self.get_toolkit()
        self.prefix_manager = PrefixManager()
        self.jsonld = get_jsonld_context()
        self.prefixes = self.get_all_prefixes(self.jsonld)
        self.required_node_properties = self.get_required_node_properties()
        self.required_edge_properties = self.get_required_edge_properties()

    def __call__(self, entity_type: GraphEntityType, rec: List):
        """
        Transformer 'inspector' Callable
        """
        if self.progress_monitor:
            self.progress_monitor(entity_type, rec)
        if entity_type == GraphEntityType.EDGE:
            self.analyse_edge(*rec)
        elif entity_type == GraphEntityType.NODE:
            self.analyse_node(*rec)
        else:
            raise RuntimeError("Unexpected GraphEntityType: " + str(entity_type))

    def get_validating_toolkit(self):
        """
        Get Validating Biolink Model toolkit
        """
        return self.validating_toolkit

    def get_validation_model_version(self):
        """
        Get Validating Biolink Model version
        """
        return self.validating_toolkit.get_model_version()

    _currently_active_toolkit: Optional[Toolkit] = None

    @classmethod
    def set_biolink_model(cls, version: Optional[str]):
        """
        Set Biolink Model version of Validator Toolkit
        """
        cls._currently_active_toolkit = get_toolkit(biolink_release=version)

    @classmethod
    def get_toolkit(cls) -> Toolkit:
        """
        Get the current default Validator Toolkit
        """
        if not cls._currently_active_toolkit:
            cls._currently_active_toolkit = get_toolkit()
        return cls._currently_active_toolkit

    _default_model_version = None

    @classmethod
    def get_default_model_version(cls):
        """
        Get the Default Biolink Model version
        """
        if not cls._default_model_version:
            # get default Biolink version from BMT
            cls._default_model_version = get_toolkit().get_model_version()
        return cls._default_model_version

    def analyse_node(self, n, data):
        """
        Analyse Node
        """
        self.validate_node_properties(n, data, self.required_node_properties)
        self.validate_node_property_types(
            n, data, toolkit=self.validating_toolkit
        )
        self.validate_node_property_values(n, data)
        self.validate_categories(n, data, toolkit=self.validating_toolkit)

    def analyse_edge(self, u, v, k, data):
        """
        Analyse edge
        """
        self.validate_edge_properties(
            u, v, data, self.required_edge_properties
        )
        self.validate_edge_property_types(
            u, v, data, toolkit=self.validating_toolkit
        )
        self.validate_edge_property_values(u, v, data)
        self.validate_edge_predicate(
            u, v, data, toolkit=self.validating_toolkit
        )

    @staticmethod
    def get_all_prefixes(jsonld: Optional[Dict] = None) -> set:
        """
        Get all prefixes from Biolink Model JSON-LD context.

        It also sets ``self.prefixes`` for subsequent access.

        Parameters
        ---------
        jsonld: Optional[Dict]
            The JSON-LD context

        Returns
        -------
        Optional[Dict]
            A set of prefixes

        """
        if not jsonld:
            jsonld = get_jsonld_context()
        prefixes: Set = set(
            k
            for k, v in jsonld.items()
            if isinstance(v, str)
            or (isinstance(v, dict) and v.setdefault("@prefix", False))
        )  # @type: ignored
        if "biolink" not in prefixes:
            prefixes.add("biolink")
        return prefixes

    @staticmethod
    def get_required_node_properties(toolkit: Optional[Toolkit] = None) -> list:
        """
        Get all properties for a node that are required, as defined by Biolink Model.

        Parameters
        ----------
        toolkit: Optional[Toolkit]
            Optional externally provided toolkit (default: use Validator class defined toolkit)

        Returns
        -------
        list
            A list of required node properties

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()
        node_properties = toolkit.get_all_node_properties()
        # TODO: remove this append statement when Biolink 3.1.3 is released - need to add domain:entity to id slot.
        # node_properties.append("id")
        required_properties = []
        for p in node_properties:
            element = toolkit.get_element(p)
            if element and element.deprecated is None:
                if (hasattr(element, "required") and element.required) or element.name == "category":
                    formatted_name = sentencecase_to_snakecase(element.name)
                    required_properties.append(formatted_name)
        return required_properties

    @staticmethod
    def get_required_edge_properties(toolkit: Optional[Toolkit] = None) -> list:
        """
        Get all properties for an edge that are required, as defined by Biolink Model.

        Parameters
        ----------
        toolkit: Optional[Toolkit]
            Optional externally provided toolkit (default: use Validator class defined toolkit)

        Returns
        -------
        list
            A list of required edge properties

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()
        edge_properties = toolkit.get_all_edge_properties()
        # TODO: remove this append statement when Biolink 3.1.3 is released - need to add domain:entity to id slot.
        edge_properties.append("id")
        required_properties = []
        for p in edge_properties:
            element = toolkit.get_element(p)
            if element and element.deprecated is None:
                if hasattr(element, "required") and element.required:
                    formatted_name = sentencecase_to_snakecase(element.name)
                    required_properties.append(formatted_name)
        return required_properties

    def validate(self, graph: BaseGraph):
        """
        Validate nodes and edges in a graph.

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to validate

        """
        self.validate_nodes(graph)
        self.validate_edges(graph)

    def validate_nodes(self, graph: BaseGraph):
        """
        Validate all the nodes in a graph.

        This method validates for the following,
        - Node properties
        - Node property type
        - Node property value type
        - Node categories

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to validate

        """
        with click.progressbar(
                graph.nodes(data=True), label="Validating nodes in graph"
        ) as bar:
            for n, data in bar:
                self.analyse_node(n, data)

    def validate_edges(self, graph: BaseGraph):
        """
        Validate all the edges in a graph.

        This method validates for the following,
        - Edge properties
        - Edge property type
        - Edge property value type
        - Edge predicate

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to validate

        """
        with click.progressbar(
                graph.edges(data=True), label="Validate edges in graph"
        ) as bar:
            for u, v, data in bar:
                self.analyse_edge(u, v, None, data)

    def validate_node_properties(
            self,
            node: str,
            data: dict,
            required_properties: list
    ):
        """
        Checks if all the required node properties exist for a given node.

        Parameters
        ----------
        node: str
            Node identifier
        data: dict
            Node properties
        required_properties: list
            Required node properties

        """
        for p in required_properties:
            if p not in data:
                error_type = ErrorType.MISSING_NODE_PROPERTY
                message = f"Required node property '{p}' is missing"
                self.log_error(node, error_type, message, MessageLevel.ERROR)

    def validate_edge_properties(
            self,
            subject: str,
            object: str,
            data: dict,
            required_properties: list
    ):
        """
        Checks if all the required edge properties exist for a given edge.

        Parameters
        ----------
        subject: str
            Subject identifier
        object: str
            Object identifier
        data: dict
            Edge properties
        required_properties: list
            Required edge properties

        """
        for p in required_properties:
            if p not in data:
                if p == "association_id":
                    # check for 'id' property instead
                    if "id" not in data:
                        error_type = ErrorType.MISSING_EDGE_PROPERTY
                        message = f"Required edge property '{p}' is missing"
                        self.log_error(
                            f"{subject}->{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR
                        )
                else:
                    error_type = ErrorType.MISSING_EDGE_PROPERTY
                    message = f"Required edge property '{p}' is missing"
                    self.log_error(
                        f"{subject}->{object}",
                        error_type,
                        message,
                        MessageLevel.ERROR
                    )

    def validate_node_property_types(
            self,
            node: str,
            data: dict,
            toolkit: Optional[Toolkit] = None
    ):
        """
        Checks if node properties have the expected value type.

        Parameters
        ----------
        node: str
            Node identifier
        data: dict
            Node properties
        toolkit: Optional[Toolkit]
            Optional externally provided toolkit (default: use Validator class defined toolkit)

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()

        error_type = ErrorType.INVALID_NODE_PROPERTY_VALUE_TYPE
        if not isinstance(node, str):
            message = "Node property 'id' is expected to be of type 'string'"
            self.log_error(node, error_type, message, MessageLevel.ERROR)

        for key, value in data.items():
            element = toolkit.get_element(key)
            if element:
                if hasattr(element, "typeof"):
                    if (element.typeof == "string" and not isinstance(value, str)) or \
                            (element.typeof == "double" and not isinstance(value, (int, float))):
                        message = f"Node property '{key}' is expected to be of type '{element.typeof}'"
                        self.log_error(node, error_type, message, MessageLevel.ERROR)
                    elif (
                            element.typeof == "uriorcurie"
                            and not isinstance(value, str)
                            and not validators.url(value)
                    ):
                        message = f"Node property '{key}' is expected to be of type 'uri' or 'CURIE'"
                        self.log_error(node, error_type, message, MessageLevel.ERROR)
                    else:
                        logger.warning(
                            f"Skipping validation for Node property '{key}'. "
                            f"Expected type '{element.typeof}' v/s Actual type '{type(value)}'"
                        )
                if hasattr(element, "multivalued"):
                    if element.multivalued:
                        if not isinstance(value, list):
                            message = f"Multi-valued node property '{key}' is expected to be of type '{list}'"
                            self.log_error(node, error_type, message, MessageLevel.ERROR)
                    else:
                        if isinstance(value, (list, set, tuple)):
                            message = f"Single-valued node property '{key}' is expected to be of type '{str}'"
                            self.log_error(node, error_type, message, MessageLevel.ERROR)

    def validate_edge_property_types(
            self,
            subject: str,
            object: str,
            data: dict,
            toolkit: Optional[Toolkit] = None
    ):
        """
        Checks if edge properties have the expected value type.

        Parameters
        ----------
        subject: str
            Subject identifier
        object: str
            Object identifier
        data: dict
            Edge properties
        toolkit: Optional[Toolkit]
            Optional externally provided toolkit (default: use Validator class defined toolkit)

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()

        error_type = ErrorType.INVALID_EDGE_PROPERTY_VALUE_TYPE
        if not isinstance(subject, str):
            message = "'subject' of an edge is expected to be of type 'string'"
            self.log_error(
                f"{subject}->{object}", error_type, message, MessageLevel.ERROR
            )
        if not isinstance(object, str):
            message = "'object' of an edge is expected to be of type 'string'"
            self.log_error(
                f"{subject}->{object}", error_type, message, MessageLevel.ERROR
            )

        for key, value in data.items():
            element = toolkit.get_element(key)
            if element:
                if hasattr(element, "typeof"):
                    if element.typeof == "string" and not isinstance(value, str):
                        message = (
                            f"Edge property '{key}' is expected to be of type 'string'"
                        )
                        self.log_error(
                            f"{subject}->{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR,
                        )
                    elif (
                            element.typeof == "uriorcurie"
                            and not isinstance(value, str)
                            and not validators.url(value)
                    ):
                        message = f"Edge property '{key}' is expected to be of type 'uri' or 'CURIE'"
                        self.log_error(
                            f"{subject}->{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR,
                        )
                    elif element.typeof == "double" and not isinstance(
                            value, (int, float)
                    ):
                        message = (
                            f"Edge property '{key}' is expected to be of type 'double'"
                        )
                        self.log_error(
                            f"{subject}->{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR,
                        )
                    else:
                        logger.warning(
                            "Skipping validation for Edge property '{}'. Expected type '{}' v/s Actual type '{}'".format(
                                key, element.typeof, type(value)
                            )
                        )
                if hasattr(element, "multivalued"):
                    if element.multivalued:
                        if not isinstance(value, list):
                            message = f"Multi-valued edge property '{key}' is expected to be of type 'list'"
                            self.log_error(
                                f"{subject}->{object}",
                                error_type,
                                message,
                                MessageLevel.ERROR,
                            )
                    else:
                        if isinstance(value, (list, set, tuple)):
                            message = f"Single-valued edge property '{key}' is expected to be of type 'str'"
                            self.log_error(
                                f"{subject}->{object}",
                                error_type,
                                message,
                                MessageLevel.ERROR,
                            )

    def validate_node_property_values(
            self,
            node: str,
            data: dict
    ):
        """
        Validate a node property's value.

        Parameters
        ----------
        node: str
            Node identifier
        data: dict
            Node properties

        """
        error_type = ErrorType.INVALID_NODE_PROPERTY_VALUE
        if not PrefixManager.is_curie(node):
            message = f"Node property 'id' is expected to be of type 'CURIE'"
            self.log_error(node, error_type, message, MessageLevel.ERROR)
        else:
            prefix = PrefixManager.get_prefix(node)
            if prefix and prefix not in self.prefixes:
                message = f"Node property 'id' has a value '{node}' with a CURIE prefix '{prefix}'" + \
                          f" is not represented in Biolink Model JSON-LD context"
                self.log_error(node, error_type, message, MessageLevel.ERROR)

    def validate_edge_property_values(
            self,
            subject: str,
            object: str,
            data: dict
    ):
        """
        Validate an edge property's value.

        Parameters
        ----------
        subject: str
            Subject identifier
        object: str
            Object identifier
        data: dict
            Edge properties

        """
        error_type = ErrorType.INVALID_EDGE_PROPERTY_VALUE
        prefixes = self.prefixes

        if PrefixManager.is_curie(subject):
            prefix = PrefixManager.get_prefix(subject)
            if prefix and prefix not in prefixes:
                message = f"Edge property 'subject' has a value '{subject}' with a CURIE prefix " + \
                          f"'{prefix}' that is not represented in Biolink Model JSON-LD context"
                self.log_error(f"{subject}->{object}", error_type, message, MessageLevel.ERROR)
        else:
            message = f"Edge property 'subject' has a value '{subject}' which is not a proper CURIE"
            self.log_error(f"{subject}->{object}", error_type, message, MessageLevel.ERROR)

        if PrefixManager.is_curie(object):
            prefix = PrefixManager.get_prefix(object)
            if prefix not in prefixes:
                message = f"Edge property 'object' has a value '{object}' with a CURIE " + \
                          f"prefix '{prefix}' that is not represented in Biolink Model JSON-LD context"
                self.log_error(f"{subject}->{object}", error_type, message, MessageLevel.ERROR)
        else:
            message = f"Edge property 'object' has a value '{object}' which is not a proper CURIE"
            self.log_error(f"{subject}->{object}", error_type, message, MessageLevel.ERROR)

    def validate_categories(
            self,
            node: str,
            data: dict,
            toolkit: Optional[Toolkit] = None
    ):
        """
        Validate ``category`` field of a given node.

        Parameters
        ----------
        node: str
            Node identifier
        data: dict
            Node properties
        toolkit: Optional[Toolkit]
            Optional externally provided toolkit (default: use Validator class defined toolkit)

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()

        error_type = ErrorType.INVALID_CATEGORY
        categories = data.get("category")
        if categories is None:
            message = "Node does not have a 'category' property"
            self.log_error(node, error_type, message, MessageLevel.ERROR)
        elif not isinstance(categories, list):
            message = f"Node property 'category' is expected to be of type {list}"
            self.log_error(node, error_type, message, MessageLevel.ERROR)
        else:
            for category in categories:
                if PrefixManager.is_curie(category):
                    category = PrefixManager.get_reference(category)
                m = re.match(r"^([A-Z][a-z\d]+)+$", category)
                if not m:
                    # category is not CamelCase
                    error_type = ErrorType.INVALID_CATEGORY
                    message = f"Category '{category}' is not in CamelCase form"
                    self.log_error(node, error_type, message, MessageLevel.ERROR)
                formatted_category = camelcase_to_sentencecase(category)
                if toolkit.is_mixin(formatted_category):
                    message = f"Category '{category}' is a mixin in the Biolink Model"
                    self.log_error(node, error_type, message, MessageLevel.ERROR)
                elif not toolkit.is_category(formatted_category):
                    message = (
                        f"Category '{category}' is unknown in the current Biolink Model"
                    )
                    self.log_error(node, error_type, message, MessageLevel.ERROR)
                else:
                    c = toolkit.get_element(formatted_category.lower())
                    if c:
                        if category != c.name and category in c.aliases:
                            message = f"Category {category} is actually an alias for {c.name}; " + \
                                      f"Should replace '{category}' with '{c.name}'"
                            self.log_error(node, error_type, message, MessageLevel.ERROR)

    def validate_edge_predicate(
            self,
            subject: str,
            object: str,
            data: dict,
            toolkit: Optional[Toolkit] = None
    ):
        """
        Validate ``edge_predicate`` field of a given edge.

        Parameters
        ----------
        subject: str
            Subject identifier
        object: str
            Object identifier
        data: dict
            Edge properties
        toolkit: Optional[Toolkit]
            Optional externally provided toolkit (default: use Validator class defined toolkit)

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()

        error_type = ErrorType.INVALID_EDGE_PREDICATE
        edge_predicate = data.get("predicate")
        if edge_predicate is None:
            message = "Edge does not have an 'predicate' property"
            self.log_error(f"{subject}->{object}", error_type, message, MessageLevel.ERROR)
        elif not isinstance(edge_predicate, str):
            message = f"Edge property 'edge_predicate' is expected to be of type 'string'"
            self.log_error(f"{subject}->{object}", error_type, message, MessageLevel.ERROR)
        else:
            if PrefixManager.is_curie(edge_predicate):
                edge_predicate = PrefixManager.get_reference(edge_predicate)
            m = re.match(r"^([a-z_][^A-Z\s]+_?[a-z_][^A-Z\s]+)+$", edge_predicate)
            if m:
                p = toolkit.get_element(snakecase_to_sentencecase(edge_predicate))
                if p is None:
                    message = f"Edge predicate '{edge_predicate}' is not in Biolink Model"
                    self.log_error(
                        f"{subject}->{object}",
                        error_type,
                        message,
                        MessageLevel.ERROR,
                    )
                elif edge_predicate != p.name and edge_predicate in p.aliases:
                    message = f"Edge predicate '{edge_predicate}' is actually an alias for {p.name}; " + \
                              f"Should replace {edge_predicate} with {p.name}"
                    self.log_error(
                        f"{subject}->{object}",
                        error_type,
                        message,
                        MessageLevel.ERROR,
                    )
            else:
                message = f"Edge predicate '{edge_predicate}' is not in snake_case form"
                self.log_error(f"{subject}->{object}", error_type, message, MessageLevel.ERROR)
