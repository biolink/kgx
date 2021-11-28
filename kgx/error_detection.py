"""
Shared graph model error reporting code currently used in
the validator, summarize_graph and meta_knowledge_graph modules
"""
from enum import Enum
from json import dump as json_dump
from sys import stderr
from typing import Dict, List, Optional, TextIO


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


class ErrorDetecting(object):
    """
    Class of object which can capture internal graph parsing error events.
    
    Superclass parent of KGX 'validate' and 'graph-summary'
    operational classes (perhaps more KGX operations later?)
    """
    def __init__(self, error_log=stderr):
        """
        Run KGX validator on an input file to check for Biolink Model compliance.

        Parameters
        ----------
        error_log: str or TextIO handle
            Output target for logging.
            
        Returns
        -------
        Dict
            A dictionary of entities which have parse errors indexed by [message_level][error_type][message]

        """
        self.errors: Dict[
            MessageLevel,
            Dict[
                ErrorType,
                Dict[
                    str,
                    List[str]
                ]
            ]
        ] = dict()

        if error_log:
            if isinstance(error_log, str):
                self.error_log = open(error_log, mode="w")
            else:
                self.error_log = error_log
        else:
            self.error_log = None
    
    def clear_errors(self):
        """
        Clears the current error log list
        """
        self.errors.clear()

    def log_error(
        self,
        entity: str,
        error_type: ErrorType,
        message: str,
        message_level: MessageLevel = MessageLevel.ERROR,
    ):
        """
        Log an error to the list of such errors.
        
        :param entity: source of parse error
        :param error_type: ValidationError ErrorType,
        :param message: message string describing the error
        :param  message_level: ValidationError MessageLevel
        """
        # index errors by entity identifier
        level = message_level.name
        error = error_type.name
        
        # clean up entity name string...
        entity = entity.strip()

        if level not in self.errors:
            self.errors[level] = dict()

        if error not in self.errors[level]:
            self.errors[level][error] = dict()
        
        # don't record duplicate instances of error type and
        # messages for entity identifiers...
        if message not in self.errors[level][error]:
            self.errors[level][error][message] = [entity]
        else:
            if entity not in self.errors[level][error][message]:
                self.errors[level][error][message].append(entity)

    def get_errors(self) -> Dict:
        """
        Get the index list of distinct error messages.

        Returns
        -------
        Dict
            A raw dictionary of entities indexed by [message_level][error_type][message]

        """
        return self.errors

    def write_report(self, outstream: Optional[TextIO] = None) -> None:
        """
        Write error get_errors to a file

        Parameters
        ----------
        outstream: TextIO
            The stream to which to write

        """
        # default error log used if not given
        if not outstream and self.error_log:
            outstream = self.error_log
        else:
            # safe here to default to stderr?
            outstream = stderr
            
        json_dump(self.get_errors(), outstream, indent=4)
        outstream.write("\n")  # print a trailing newline(?)
