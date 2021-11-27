"""
KGX Package

Some global error handling classes placed here in the default package.
"""
__version__ = "1.5.4"

from enum import Enum
from typing import List
from sys import stderr

from kgx.config import get_logger

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
    MISSING_NODE_CURIE_PREFIX = 11
    DUPLICATE_NODE = 12
    MISSING_NODE = 13,
    INVALID_EDGE_TRIPLE = 14,
    VALIDATION_SYSTEM_ERROR = 99


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
    error_type: kgx.ErrorType
        The nature of the error
    message: str
        The error message
    message_level: kgx.MessageLevel
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
        """
        Return Validation Error object attributes as a dictionary
        """
        return {
            "entity": self.entity,
            "error_type": self.error_type.name,
            "message": self.message,
            "message_level": self.message_level.name,
        }


class ErrorDetecting(object):
    """
    Class of object which can log internal ValidationError events.
    
    Superclass parent of KGX 'validate' and 'graph-summary'
    operational classes (perhaps more KGX operations later?)
    """
    def __init__(self, error_log=stderr):
        
        self.errors: List[ValidationError] = list()

        if error_log and isinstance(error_log, str):
            self.error_log = open(error_log, mode="w")
        else:
            self.error_log = error_log

    def get_errors(self):
        """
        Get list of ValidationError records
        """
        return self.errors

    def log_error(
        self,
        entity: str,
        error_type: ErrorType,
        message: str,
        message_level: MessageLevel,
    ):
        """
        Log an error to the list of such errors.
        
        :param entity: source of parse error
        :param error_type: ValidationError ErrorType,
        :param message: message string describing the error
        :param  message_level: ValidationError MessageLevel
        """
        ve = ValidationError(entity, error_type, message, message_level)
        self.errors.append(ve)

    # DRY parse warning message
    def parse_error(
            self,
            entity: str,
            error_type: ErrorType,
            prefix: str,
            item: str,
            suffix: str = None,
            message_level: MessageLevel = MessageLevel.ERROR,
    ):
        """
        Logs a parse warning or error.

        :param entity: source of parse error
        :param error_type: ValidationError ErrorType,
        :param prefix: message string preceeding item identifier
        :param item: item identifier
        :param suffix: message string succeeding item identifier (default: empty)
        :param  message_level: ValidationError MessageLevel
        """
        suffix = " " + suffix if suffix else ""
        errmsg = f"{prefix} '{item}' {suffix}? Ignoring..."
        self.log_error(entity, error_type, errmsg, message_level)
        logger.debug(errmsg)
        print(errmsg, file=self.error_log)
