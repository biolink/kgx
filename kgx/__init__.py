__version__ = "1.5.4"

from typing import List
from sys import stderr

from kgx.config import get_logger
from kgx.validator import ValidationError, ErrorType, MessageLevel

logger = get_logger()


class ErrorDetecting:
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
