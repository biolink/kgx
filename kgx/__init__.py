__version__ = "1.5.4"

from typing import List

from kgx.validator import ValidationError, ErrorType, MessageLevel


class ErrorDetecting:
    """
    Class of object which can log internal ValidationError events.
    
    Superclass parent of KGX 'validate' and 'graph-summary'
    operational classes (perhaps more KGX operations later?)
    """
    def __init__(self):
        self.errors: List[ValidationError] = list()
    
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
    
    def get_errors(self):
        """
        Get list of ValidationError records
        """
        return self.errors
