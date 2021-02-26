import re
from enum import Enum
from typing import Tuple, List, TextIO, Optional, Dict, Set

import click
import validators

from kgx.config import get_jsonld_context, get_logger
from kgx.graph.base_graph import BaseGraph
from kgx.utils.kgx_utils import (
    get_toolkit,
    snakecase_to_sentencecase,
    sentencecase_to_snakecase,
    camelcase_to_sentencecase,
)
from kgx.prefix_manager import PrefixManager

log = get_logger()


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
        self, entity: str, error_type: ErrorType, message: str, message_level: MessageLevel
    ):
        self.entity = entity
        self.error_type = error_type
        self.message = message
        self.message_level = message_level

    def __str__(self):
        return f"[{self.message_level.name}][{self.error_type.name}] {self.entity} - {self.message}"

    def as_dict(self):
        return {
            'entity': self.entity,
            'error_type': self.error_type.name,
            'message': self.message,
            'message_level': self.message_level.name,
        }


class Validator(object):
    """
    Class for validating a property graph.

    Parameters
    ----------
    verbose: bool
        Whether the generated report should be verbose or not (default: ``False``)

    """

    def __init__(self, verbose: bool = False):
        self.toolkit = get_toolkit()
        self.prefix_manager = PrefixManager()
        self.jsonld = get_jsonld_context()
        self.prefixes = Validator.get_all_prefixes(self.jsonld)
        self.required_node_properties = Validator.get_required_node_properties()
        self.required_edge_properties = Validator.get_required_edge_properties()
        self.verbose = verbose

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
        prefixes: Set = set(k for k, v in jsonld.items() if isinstance(v, str))  # type: ignore
        if 'biolink' not in prefixes:
            prefixes.add('biolink')
        return prefixes

    @staticmethod
    def get_required_node_properties() -> list:
        """
        Get all properties for a node that are required, as defined by Biolink Model.

        Returns
        -------
        list
            A list of required node properties

        """
        toolkit = get_toolkit()
        node_properties = toolkit.get_all_node_properties()
        required_properties = []
        for p in node_properties:
            element = toolkit.get_element(p)
            if element and element.deprecated is None:
                if hasattr(element, 'required') and element.required:
                    formatted_name = sentencecase_to_snakecase(element.name)
                    required_properties.append(formatted_name)
        return required_properties

    @staticmethod
    def get_required_edge_properties() -> list:
        """
        Get all properties for an edge that are required, as defined by Biolink Model.

        Returns
        -------
        list
            A list of required edge properties

        """
        toolkit = get_toolkit()
        edge_properties = toolkit.get_all_edge_properties()
        required_properties = []
        for p in edge_properties:
            element = toolkit.get_element(p)
            if element and element.deprecated is None:
                if hasattr(element, 'required') and element.required:
                    formatted_name = sentencecase_to_snakecase(element.name)
                    required_properties.append(formatted_name)
        print(required_properties)
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
        return node_errors + edge_errors

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
        with click.progressbar(graph.nodes(data=True), label='Validating nodes in graph') as bar:
            for n, data in bar:
                e1 = Validator.validate_node_properties(n, data, self.required_node_properties)
                e2 = Validator.validate_node_property_types(n, data)
                e3 = Validator.validate_node_property_values(n, data)
                e4 = Validator.validate_categories(n, data)
                errors += e1 + e2 + e3 + e4
        return errors

    def validate_edges(self, graph: BaseGraph) -> list:
        """
        Validate all the edges in a graph.

        This method validates for the following,
        - Edge properties
        - Edge property type
        - Edge property value type
        - Edge label

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
        with click.progressbar(graph.edges(data=True), label='Validate edges in graph') as bar:
            for u, v, data in bar:
                e1 = Validator.validate_edge_properties(u, v, data, self.required_edge_properties)
                e2 = Validator.validate_edge_property_types(u, v, data)
                e3 = Validator.validate_edge_property_values(u, v, data)
                e4 = Validator.validate_edge_predicate(u, v, data)
                errors += e1 + e2 + e3 + e4
        return errors

    @staticmethod
    def validate_node_properties(node: str, data: dict, required_properties: list) -> list:
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
                errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))
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
                if p == 'association_id':
                    # check for 'id' property instead
                    if 'id' not in data:
                        error_type = ErrorType.MISSING_EDGE_PROPERTY
                        message = f"Required edge property '{p}' missing"
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                            )
                        )
                else:
                    error_type = ErrorType.MISSING_EDGE_PROPERTY
                    message = f"Required edge property '{p}' missing"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                        )
                    )
        return errors

    @staticmethod
    def validate_node_property_types(node: str, data: dict) -> list:
        """
        Checks if node properties have the expected value type.

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
        toolkit = get_toolkit()
        errors = []
        error_type = ErrorType.INVALID_NODE_PROPERTY_VALUE_TYPE
        if not isinstance(node, str):
            message = "Node property 'id' expected to be of type 'string'"
            errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))

        for key, value in data.items():
            element = toolkit.get_element(key)
            if element:
                if hasattr(element, 'typeof'):
                    if element.typeof == 'string' and not isinstance(value, str):
                        message = f"Node property '{key}' expected to be of type '{element.typeof}'"
                        errors.append(
                            ValidationError(node, error_type, message, MessageLevel.ERROR)
                        )
                    elif (
                        element.typeof == 'uriorcurie'
                        and not isinstance(value, str)
                        and not validators.url(value)
                    ):
                        message = f"Node property '{key}' expected to be of type 'uri' or 'CURIE'"
                        errors.append(
                            ValidationError(node, error_type, message, MessageLevel.ERROR)
                        )
                    elif element.typeof == 'double' and not isinstance(value, (int, float)):
                        message = f"Node property '{key}' expected to be of type '{element.typeof}'"
                        errors.append(
                            ValidationError(node, error_type, message, MessageLevel.ERROR)
                        )
                    else:
                        log.warning(
                            "Skipping validation for Node property '{}'. Expected type '{}' vs Actual type '{}'".format(
                                key, element.typeof, type(value)
                            )
                        )
                if hasattr(element, 'multivalued'):
                    if element.multivalued:
                        if not isinstance(value, list):
                            message = f"Multi-valued node property '{key}' expected to be of type '{list}'"
                            errors.append(
                                ValidationError(node, error_type, message, MessageLevel.ERROR)
                            )
                    else:
                        if isinstance(value, (list, set, tuple)):
                            message = f"Single-valued node property '{key}' expected to be of type '{str}'"
                            errors.append(
                                ValidationError(node, error_type, message, MessageLevel.ERROR)
                            )
        return errors

    @staticmethod
    def validate_edge_property_types(subject: str, object: str, data: dict) -> list:
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

        Returns
        -------
        list
            A list of errors for a given edge

        """
        toolkit = get_toolkit()
        errors = []
        error_type = ErrorType.INVALID_EDGE_PROPERTY_VALUE_TYPE
        if not isinstance(subject, str):
            message = "'subject' of an edge expected to be of type 'string'"
            errors.append(
                ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
            )
        if not isinstance(object, str):
            message = "'object' of an edge expected to be of type 'string'"
            errors.append(
                ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
            )

        for key, value in data.items():
            element = toolkit.get_element(key)
            if element:
                if hasattr(element, 'typeof'):
                    if element.typeof == 'string' and not isinstance(value, str):
                        message = f"Edge property '{key}' expected to be of type 'string'"
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                            )
                        )
                    elif (
                        element.typeof == 'uriorcurie'
                        and not isinstance(value, str)
                        and not validators.url(value)
                    ):
                        message = f"Edge property '{key}' expected to be of type 'uri' or 'CURIE'"
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                            )
                        )
                    elif element.typeof == 'double' and not isinstance(value, (int, float)):
                        message = f"Edge property '{key}' expected to be of type 'double'"
                        errors.append(
                            ValidationError(
                                f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                            )
                        )
                    else:
                        log.warning(
                            "Skipping validation for Edge property '{}'. Expected type '{}' vs Actual type '{}'".format(
                                key, element.typeof, type(value)
                            )
                        )
                if hasattr(element, 'multivalued'):
                    if element.multivalued:
                        if not isinstance(value, list):
                            message = (
                                f"Multi-valued edge property '{key}' expected to be of type 'list'"
                            )
                            errors.append(
                                ValidationError(
                                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                                )
                            )
                    else:
                        if isinstance(value, (list, set, tuple)):
                            message = (
                                f"Single-valued edge property '{key}' expected to be of type 'str'"
                            )
                            errors.append(
                                ValidationError(
                                    f"{subject}-{object}", error_type, message, MessageLevel.ERROR
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
            errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))
        else:
            prefix = PrefixManager.get_prefix(node)
            if prefix and prefix not in Validator.get_all_prefixes():
                message = f"Node property 'id' has a value '{node}' with a CURIE prefix '{prefix}' is not represented in Biolink Model JSON-LD context"
                errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))
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
                    ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
                )
        else:
            message = f"Edge property 'subject' has a value '{subject}' which is not a proper CURIE"
            errors.append(
                ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
            )

        if PrefixManager.is_curie(object):
            prefix = PrefixManager.get_prefix(object)
            if prefix not in prefixes:
                message = f"Edge property 'object' has a value '{object}' with a CURIE prefix '{prefix}' that is not represented in Biolink Model JSON-LD context"
                errors.append(
                    ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
                )
        else:
            message = f"Edge property 'object' has a value '{object}' which is not a proper CURIE"
            errors.append(
                ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
            )
        if 'relation' in data:
            if PrefixManager.is_curie(data['relation']):
                prefix = PrefixManager.get_prefix(data['relation'])
                if prefix not in prefixes:
                    message = f"Edge property 'relation' has a value '{data['relation']}' with a CURIE prefix '{prefix}' that is not represented in Biolink Model JSON-LD context"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                        )
                    )
            else:
                message = f"Edge property 'relation' has a value '{data['relation']}' which is not a proper CURIE"
                errors.append(
                    ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
                )
        return errors

    @staticmethod
    def validate_categories(node: str, data: dict) -> list:
        """
        Validate ``category`` field of a given node.

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
        toolkit = get_toolkit()
        error_type = ErrorType.INVALID_CATEGORY
        errors = []
        categories = data.get('category')
        if categories is None:
            message = "Node does not have a 'category' property"
            errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))
        elif not isinstance(categories, list):
            message = f"Node property 'category' expected to be of type {list}"
            errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))
        else:
            for category in categories:
                if PrefixManager.is_curie(category):
                    category = PrefixManager.get_reference(category)
                m = re.match(r"^([A-Z][a-z\d]+)+$", category)
                if not m:
                    # category is not CamelCase
                    error_type = ErrorType.INVALID_CATEGORY
                    message = f"Category '{category}' is not in CamelCase form"
                    errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))
                formatted_category = camelcase_to_sentencecase(category)
                if not toolkit.is_category(formatted_category):
                    message = f"Category '{category}' not in Biolink Model"
                    errors.append(ValidationError(node, error_type, message, MessageLevel.ERROR))
                else:
                    c = toolkit.get_element(formatted_category.lower())
                    if c:
                        if category != c.name and category in c.aliases:
                            message = f"Category {category} is actually an alias for {c.name}; Should replace '{category}' with '{c.name}'"
                            errors.append(
                                ValidationError(node, error_type, message, MessageLevel.ERROR)
                            )
        return errors

    @staticmethod
    def validate_edge_predicate(subject: str, object: str, data: dict) -> list:
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

        Returns
        -------
        list
            A list of errors for a given edge

        """
        toolkit = get_toolkit()
        error_type = ErrorType.INVALID_EDGE_PREDICATE
        errors = []
        edge_predicate = data.get('predicate')
        if edge_predicate is None:
            message = "Edge does not have an 'predicate' property"
            errors.append(
                ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
            )
        elif not isinstance(edge_predicate, str):
            message = f"Edge property 'edge_predicate' expected to be of type 'string'"
            errors.append(
                ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
            )
        else:
            if PrefixManager.is_curie(edge_predicate):
                edge_predicate = PrefixManager.get_reference(edge_predicate)
            m = re.match(r"^([a-z_][^A-Z\s]+_?[a-z_][^A-Z\s]+)+$", edge_predicate)
            if m:
                p = toolkit.get_element(snakecase_to_sentencecase(edge_predicate))
                if p is None:
                    message = f"Edge label '{edge_predicate}' not in Biolink Model"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                        )
                    )
                elif edge_predicate != p.name and edge_predicate in p.aliases:
                    message = f"Edge label '{edge_predicate}' is actually an alias for {p.name}; Should replace {edge_predicate} with {p.name}"
                    errors.append(
                        ValidationError(
                            f"{subject}-{object}", error_type, message, MessageLevel.ERROR
                        )
                    )
            else:
                message = f"Edge label '{edge_predicate}' is not in snake_case form"
                errors.append(
                    ValidationError(f"{subject}-{object}", error_type, message, MessageLevel.ERROR)
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

    @staticmethod
    def write_report(errors: List[ValidationError], outstream: TextIO) -> None:
        """
        Write error report to a file

        Parameters
        ----------
        errors: List[ValidationError]
            List of kgx.validator.ValidationError
        outstream: TextIO
            The stream to write to

        """
        for x in Validator.report(errors):
            outstream.write(f"{x}\n")
