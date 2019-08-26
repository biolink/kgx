import re
import stringcase
from bmt import Toolkit

toolkit = None

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

def get_toolkit() -> Toolkit:
    """
    Get an instance of bmt.Toolkit
    If there no instance defined, then one is instantiated and returned.

    Returns
    -------
    bmt.Toolkit
        an instance of bmt.Toolkit

    """
    global toolkit
    if toolkit is None:
        toolkit = Toolkit()

    return toolkit

def generate_edge_key(s: str, p: str, o: str) -> str:
    """
    Generates an edge key based on a given subject, edge_label and object.

    Parameters
    ----------
    s: str
        Subject
    edge_label: str
        Edge label
    o: str
        Object

    Returns
    -------
    str
        Edge key as a string

    """
    return '{}-{}-{}'.format(s, edge_label, o)
