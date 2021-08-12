import re
from enum import Enum
from typing import List, TextIO, Optional, Dict, Set, Callable

import click
import validators
from bmt import Toolkit

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


class ErrorType(Enum):
    """
    Validation error types
    """

    MISSING_NODE_PROPERTY = 1
    MISSING_EDGE_PROPERTY = 2
    INVALID_NODE_PROPERTY_VALUE_TYPE = 3
    INVALID_NODE_PROPERTY_VALUE = 4
    INVALID_EDGE_PROPERTY_VALUE_TYPE = 5
    INVALID_EDGE_PROPERTY_VALUE = 6
    NO_CATEGORY = 7
    INVALID_CATEGORY = 8
    NO_EDGE_PREDICATE = 9
    INVALID_EDGE_PREDICATE = 10


class MessageLevel(Enum):
    """
    Message level for validation reports
    """

    # Recommendations
    INFO = 1
    # Message to convey 'should'
    WARNING = 2
    # Message to convey 'must'
    ERROR = 3


class ValidationError(object):
    """
    ValidationError class that represents an error.

    Parameters
    ----------
    entity: str
        The node or edge entity that is failing validation
    error_type: kgx.validator.ErrorType
        The nature of the error
    message: str
        The error message
    message_level: kgx.validator.MessageLevel
        The message level

    """

    def __init__(
        self,
        entity: str,
        error_type: ErrorType,
        message: str,
        message_level: MessageLevel,
    ):
        self.entity = entity
        self.error_type = error_type
        self.message = message
        self.message_level = message_level

    def __str__(self):
        return f"[{self.message_level.name}][{self.error_type.name}] {self.entity} - {self.message}"

    def as_dict(self):
        return {
            "entity": self.entity,
            "error_type": self.error_type.name,
            "message": self.message,
            "message_level": self.message_level.name,
        }


class Validator(object):
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
    """

    def __init__(
        self,
        verbose: bool = False,
        progress_monitor: Optional[Callable[[GraphEntityType, List], None]] = None,
        schema: Optional[str] = None,
    ):
        # formal arguments
        self.verbose: bool = verbose
        self.progress_monitor: Optional[
            Callable[[GraphEntityType, List], None]
        ] = progress_monitor

        # internal attributes
        # associated currently active _currently_active_toolkit with this Validator instance
        self.validating_toolkit = self.get_toolkit()
        self.prefix_manager = PrefixManager()
        self.jsonld = get_jsonld_context()
        self.prefixes = Validator.get_all_prefixes(self.jsonld)
        self.required_node_properties = Validator.get_required_node_properties()
        self.required_edge_properties = Validator.get_required_edge_properties()
        self.errors: List[ValidationError] = list()

    def __call__(self, entity_type: GraphEntityType, rec: List):
        """
        Transformer 'inspector' Callable
        """
        if self.progress_monitor:
            self.progress_monitor(entity_type, rec)
        if entity_type == GraphEntityType.EDGE:
            self.errors += self.analyse_edge(*rec)
        elif entity_type == GraphEntityType.NODE:
            self.errors += self.analyse_node(*rec)
        else:
            raise RuntimeError("Unexpected GraphEntityType: " + str(entity_type))

    def get_validating_toolkit(self):
        return self.validating_toolkit

    def get_validation_model_version(self):
        return self.validating_toolkit.get_model_version()

    def get_errors(self):
        return self.errors

    _currently_active_toolkit: Optional[Toolkit] = None

    @classmethod
    def set_biolink_model(cls, version: Optional[str]):
        cls._currently_active_toolkit = get_toolkit(biolink_release=version)

    @classmethod
    def get_toolkit(cls) -> Toolkit:
        if not cls._currently_active_toolkit:
            cls._currently_active_toolkit = get_toolkit()
        return cls._currently_active_toolkit

    _default_model_version = None

    @classmethod
    def get_default_model_version(cls):
        if not cls._default_model_version:
            # get default Biolink version from BMT
            cls._default_model_version = get_toolkit().get_model_version()
        return cls._default_model_version

    def analyse_node(self, n, data):
        e1 = Validator.validate_node_properties(n, data, self.required_node_properties)
        e2 = Validator.validate_node_property_types(
            n, data, toolkit=self.validating_toolkit
        )
        e3 = Validator.validate_node_property_values(n, data)
        e4 = Validator.validate_categories(n, data, toolkit=self.validating_toolkit)
        return e1 + e2 + e3 + e4

    def analyse_edge(self, u, v, k, data):
        e1 = Validator.validate_edge_properties(
            u, v, data, self.required_edge_properties
        )
        e2 = Validator.validate_edge_property_types(
            u, v, data, toolkit=self.validating_toolkit
        )
        e3 = Validator.validate_edge_property_values(u, v, data)
        e4 = Validator.validate_edge_predicate(
            u, v, data, toolkit=self.validating_toolkit
        )
        return e1 + e2 + e3 + e4

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
        required_properties = []
        for p in node_properties:
            element = toolkit.get_element(p)
            if element and element.deprecated is None:
                if hasattr(element, "required") and element.required:
                    formatted_name = sentencecase_to_snakecase(element.name)
                    required_properties.append(formatted_name)
                elif element.name == "category":
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
        required_properties = []
        for p in edge_properties:
            element = toolkit.get_element(p)
            if element and element.deprecated is None:
                if hasattr(element, "required") and element.required:
                    formatted_name = sentencecase_to_snakecase(element.name)
                    required_properties.append(formatted_name)
        return required_properties

    def validate(self, graph: BaseGraph) -> list:
        """
        Validate nodes and edges in a graph.
        TODO: Support strict mode

        Parameters
        ----------
        graph: kgx.graph.base_graph.BaseGraph
            The graph to validate

        Returns
        -------
        list
            A list of errors for a given graph

        """
        node_errors = self.validate_nodes(graph)
        edge_errors = self.validate_edges(graph)
        self.errors = node_errors + edge_errors
        return self.errors

    def validate_nodes(self, graph: BaseGraph) -> list:
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

        Returns
        -------
        list
            A list of errors for a given graph

        """
        errors = []
        with click.progressbar(
            graph.nodes(data=True), label="Validating nodes in graph"
        ) as bar:
            for n, data in bar:
                errors += self.analyse_node(n, data)
        return errors

    def validate_edges(self, graph: BaseGraph) -> list:
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

        Returns
        -------
        list
            A list of errors for a given graph

        """
        errors = []
        with click.progressbar(
            graph.edges(data=True), label="Validate edges in graph"
        ) as bar:
            for u, v, data in bar:
                errors += self.analyse_edge(u, v, None, data)
        return errors

    @staticmethod
    def validate_node_properties(
        node: str, data: dict, required_properties: list
    ) -> list:
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

        Returns
        -------
        list
            A list of errors for a given node

        """
        errors = []
        for p in required_properties:
            if p not in data:
                error_type = ErrorType.MISSING_NODE_PROPERTY
                message = f"Required node property '{p}' missing"
                errors.append(
                    ValidationError(node, error_type, message, MessageLevel.ERROR)
                )
        return errors

    @staticmethod
    def validate_edge_properties(
        subject: str, object: str, data: dict, required_properties: list
    ) -> list:
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

        Returns
        -------
        list
            A list of errors for a given edge

        """
        errors = []
        for p in required_properties:
            if p not in data:
                if p == "association_id":
                    # check for 'id' property instead
                    if "id" not in data:
                        error_type = ErrorType.MISSING_EDGE_PROPERTY
                        message = f"Required edge property '{p}' missing"
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}",
                                error_type,
                                message,
                                MessageLevel.ERROR,
                            )
                        )
                else:
                    error_type = ErrorType.MISSING_EDGE_PROPERTY
                    message = f"Required edge property '{p}' missing"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR,
                        )
                    )
        return errors

    @staticmethod
    def validate_node_property_types(
        node: str, data: dict, toolkit: Optional[Toolkit] = None
    ) -> list:
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

        Returns
        -------
        list
            A list of errors for a given node

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()
        errors = []
        error_type = ErrorType.INVALID_NODE_PROPERTY_VALUE_TYPE
        if not isinstance(node, str):
            message = "Node property 'id' expected to be of type 'string'"
            errors.append(
                ValidationError(node, error_type, message, MessageLevel.ERROR)
            )

        for key, value in data.items():
            element = toolkit.get_element(key)
            if element:
                if hasattr(element, "typeof"):
                    if element.typeof == "string" and not isinstance(value, str):
                        message = f"Node property '{key}' expected to be of type '{element.typeof}'"
                        errors.append(
                            ValidationError(
                                node, error_type, message, MessageLevel.ERROR
                            )
                        )
                    elif (
                        element.typeof == "uriorcurie"
                        and not isinstance(value, str)
                        and not validators.url(value)
                    ):
                        message = f"Node property '{key}' expected to be of type 'uri' or 'CURIE'"
                        errors.append(
                            ValidationError(
                                node, error_type, message, MessageLevel.ERROR
                            )
                        )
                    elif element.typeof == "double" and not isinstance(
                        value, (int, float)
                    ):
                        message = f"Node property '{key}' expected to be of type '{element.typeof}'"
                        errors.append(
                            ValidationError(
                                node, error_type, message, MessageLevel.ERROR
                            )
                        )
                    else:
                        logger.warning(
                            "Skipping validation for Node property '{}'. Expected type '{}' vs Actual type '{}'".format(
                                key, element.typeof, type(value)
                            )
                        )
                if hasattr(element, "multivalued"):
                    if element.multivalued:
                        if not isinstance(value, list):
                            message = f"Multi-valued node property '{key}' expected to be of type '{list}'"
                            errors.append(
                                ValidationError(
                                    node, error_type, message, MessageLevel.ERROR
                                )
                            )
                    else:
                        if isinstance(value, (list, set, tuple)):
                            message = f"Single-valued node property '{key}' expected to be of type '{str}'"
                            errors.append(
                                ValidationError(
                                    node, error_type, message, MessageLevel.ERROR
                                )
                            )
        return errors

    @staticmethod
    def validate_edge_property_types(
        subject: str, object: str, data: dict, toolkit: Optional[Toolkit] = None
    ) -> list:
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

        Returns
        -------
        list
            A list of errors for a given edge

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()
        errors = []
        error_type = ErrorType.INVALID_EDGE_PROPERTY_VALUE_TYPE
        if not isinstance(subject, str):
            message = "'subject' of an edge expected to be of type 'string'"
            errors.append(
                ValidationError(
                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                )
            )
        if not isinstance(object, str):
            message = "'object' of an edge expected to be of type 'string'"
            errors.append(
                ValidationError(
                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                )
            )

        for key, value in data.items():
            element = toolkit.get_element(key)
            if element:
                if hasattr(element, "typeof"):
                    if element.typeof == "string" and not isinstance(value, str):
                        message = (
                            f"Edge property '{key}' expected to be of type 'string'"
                        )
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}",
                                error_type,
                                message,
                                MessageLevel.ERROR,
                            )
                        )
                    elif (
                        element.typeof == "uriorcurie"
                        and not isinstance(value, str)
                        and not validators.url(value)
                    ):
                        message = f"Edge property '{key}' expected to be of type 'uri' or 'CURIE'"
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}",
                                error_type,
                                message,
                                MessageLevel.ERROR,
                            )
                        )
                    elif element.typeof == "double" and not isinstance(
                        value, (int, float)
                    ):
                        message = (
                            f"Edge property '{key}' expected to be of type 'double'"
                        )
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}",
                                error_type,
                                message,
                                MessageLevel.ERROR,
                            )
                        )
                    else:
                        logger.warning(
                            "Skipping validation for Edge property '{}'. Expected type '{}' vs Actual type '{}'".format(
                                key, element.typeof, type(value)
                            )
                        )
                if hasattr(element, "multivalued"):
                    if element.multivalued:
                        if not isinstance(value, list):
                            message = f"Multi-valued edge property '{key}' expected to be of type 'list'"
                            errors.append(
                                ValidationError(
                                    f"{subject}-{object}",
                                    error_type,
                                    message,
                                    MessageLevel.ERROR,
                                )
                            )
                    else:
                        if isinstance(value, (list, set, tuple)):
                            message = f"Single-valued edge property '{key}' expected to be of type 'str'"
                            errors.append(
                                ValidationError(
                                    f"{subject}-{object}",
                                    error_type,
                                    message,
                                    MessageLevel.ERROR,
                                )
                            )
        return errors

    @staticmethod
    def validate_node_property_values(node: str, data: dict) -> list:
        """
        Validate a node property's value.

        Parameters
        ----------
        node: str
            Node identifier
        data: dict
            Node properties

        Returns
        -------
        list
            A list of errors for a given node

        """
        errors = []
        error_type = ErrorType.INVALID_NODE_PROPERTY_VALUE
        if not PrefixManager.is_curie(node):
            message = f"Node property 'id' expected to be of type 'CURIE'"
            errors.append(
                ValidationError(node, error_type, message, MessageLevel.ERROR)
            )
        else:
            prefix = PrefixManager.get_prefix(node)
            if prefix and prefix not in Validator.get_all_prefixes():
                message = f"Node property 'id' has a value '{node}' with a CURIE prefix '{prefix}' is not represented in Biolink Model JSON-LD context"
                errors.append(
                    ValidationError(node, error_type, message, MessageLevel.ERROR)
                )
        return errors

    @staticmethod
    def validate_edge_property_values(subject: str, object: str, data: dict) -> list:
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

        Returns
        -------
        list
            A list of errors for a given edge

        """
        errors = []
        error_type = ErrorType.INVALID_EDGE_PROPERTY_VALUE
        prefixes = Validator.get_all_prefixes()

        if PrefixManager.is_curie(subject):
            prefix = PrefixManager.get_prefix(subject)
            if prefix and prefix not in prefixes:
                message = f"Edge property 'subject' has a value '{subject}' with a CURIE prefix '{prefix}' that is not represented in Biolink Model JSON-LD context"
                errors.append(
                    ValidationError(
                        f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                    )
                )
        else:
            message = f"Edge property 'subject' has a value '{subject}' which is not a proper CURIE"
            errors.append(
                ValidationError(
                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                )
            )

        if PrefixManager.is_curie(object):
            prefix = PrefixManager.get_prefix(object)
            if prefix not in prefixes:
                message = f"Edge property 'object' has a value '{object}' with a CURIE prefix '{prefix}' that is not represented in Biolink Model JSON-LD context"
                errors.append(
                    ValidationError(
                        f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                    )
                )
        else:
            message = f"Edge property 'object' has a value '{object}' which is not a proper CURIE"
            errors.append(
                ValidationError(
                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                )
            )
        if "relation" in data:
            if PrefixManager.is_curie(data["relation"]):
                prefix = PrefixManager.get_prefix(data["relation"])
                if prefix not in prefixes:
                    message = f"Edge property 'relation' has a value '{data['relation']}' with a CURIE prefix '{prefix}' that is not represented in Biolink Model JSON-LD context"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR,
                        )
                    )
            else:
                message = f"Edge property 'relation' has a value '{data['relation']}' which is not a proper CURIE"
                errors.append(
                    ValidationError(
                        f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                    )
                )
        return errors

    @staticmethod
    def validate_categories(
        node: str, data: dict, toolkit: Optional[Toolkit] = None
    ) -> list:
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

        Returns
        -------
        list
            A list of errors for a given node

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()
        error_type = ErrorType.INVALID_CATEGORY
        errors = []
        categories = data.get("category")
        if categories is None:
            message = "Node does not have a 'category' property"
            errors.append(
                ValidationError(node, error_type, message, MessageLevel.ERROR)
            )
        elif not isinstance(categories, list):
            message = f"Node property 'category' expected to be of type {list}"
            errors.append(
                ValidationError(node, error_type, message, MessageLevel.ERROR)
            )
        else:
            for category in categories:
                if PrefixManager.is_curie(category):
                    category = PrefixManager.get_reference(category)
                m = re.match(r"^([A-Z][a-z\d]+)+$", category)
                if not m:
                    # category is not CamelCase
                    error_type = ErrorType.INVALID_CATEGORY
                    message = f"Category '{category}' is not in CamelCase form"
                    errors.append(
                        ValidationError(node, error_type, message, MessageLevel.ERROR)
                    )
                formatted_category = camelcase_to_sentencecase(category)
                if toolkit.is_mixin(formatted_category):
                    message = f"Category '{category}' is a mixin in the Biolink Model"
                    errors.append(
                        ValidationError(node, error_type, message, MessageLevel.ERROR)
                    )
                elif not toolkit.is_category(formatted_category):
                    message = (
                        f"Category '{category}' unknown in the current Biolink Model"
                    )
                    errors.append(
                        ValidationError(node, error_type, message, MessageLevel.ERROR)
                    )
                else:
                    c = toolkit.get_element(formatted_category.lower())
                    if c:
                        if category != c.name and category in c.aliases:
                            message = f"Category {category} is actually an alias for {c.name}; Should replace '{category}' with '{c.name}'"
                            errors.append(
                                ValidationError(
                                    node, error_type, message, MessageLevel.ERROR
                                )
                            )
        return errors

    @staticmethod
    def validate_edge_predicate(
        subject: str, object: str, data: dict, toolkit: Optional[Toolkit] = None
    ) -> list:
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

        Returns
        -------
        list
            A list of errors for a given edge

        """
        if not toolkit:
            toolkit = Validator.get_toolkit()
        error_type = ErrorType.INVALID_EDGE_PREDICATE
        errors = []
        edge_predicate = data.get("predicate")
        if edge_predicate is None:
            message = "Edge does not have an 'predicate' property"
            errors.append(
                ValidationError(
                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                )
            )
        elif not isinstance(edge_predicate, str):
            message = f"Edge property 'edge_predicate' expected to be of type 'string'"
            errors.append(
                ValidationError(
                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                )
            )
        else:
            if PrefixManager.is_curie(edge_predicate):
                edge_predicate = PrefixManager.get_reference(edge_predicate)
            m = re.match(r"^([a-z_][^A-Z\s]+_?[a-z_][^A-Z\s]+)+$", edge_predicate)
            if m:
                p = toolkit.get_element(snakecase_to_sentencecase(edge_predicate))
                if p is None:
                    message = f"Edge predicate '{edge_predicate}' not in Biolink Model"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR,
                        )
                    )
                elif edge_predicate != p.name and edge_predicate in p.aliases:
                    message = f"Edge predicate '{edge_predicate}' is actually an alias for {p.name}; Should replace {edge_predicate} with {p.name}"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}",
                            error_type,
                            message,
                            MessageLevel.ERROR,
                        )
                    )
            else:
                message = f"Edge predicate '{edge_predicate}' is not in snake_case form"
                errors.append(
                    ValidationError(
                        f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                    )
                )
        return errors

    @staticmethod
    def report(errors: List[ValidationError]) -> List:
        """
        Prepare error report.

        Parameters
        ----------
        errors: List[ValidationError]
            List of kgx.validator.ValidationError

        Returns
        -------
        List
            A list of formatted errors

        """
        return [str(x) for x in errors]

    def get_error_messages(self):
        """
        A direct Validator "instance" method version of report()
        that directly accesses the internal Validator self.errors list.

        Returns
        -------
        List
            A list of formatted error messages.

        """
        return Validator.report(self.errors)

    def write_report(self, outstream: TextIO) -> None:
        """
        Write error report to a file

        Parameters
        ----------
        outstream: TextIO
            The stream to write to

        """
        for x in Validator.report(self.errors):
            outstream.write(f"{x}\n")
