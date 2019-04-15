import re
import stringcase

def un_camel_case(s: str) -> str:
    """
    Convert CamelCase to normal string.

    Parameters
    ----------
    s: str
        Input string in CamelCase

    Returns
    -------
    str
        a normal string

    """
    return stringcase.sentencecase(s).lower()
