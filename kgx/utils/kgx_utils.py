import re
import stringcase
from bmt import Toolkit

toolkit = None

def camelcase_to_sentencecase(s: str) -> str:
    """
    Convert CamelCase to sentence case.

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

def snakecase_to_sentencecase(s: str) -> str:
    """
    Convert snake_case to sentence case.

    Parameters
    ----------
    s: str
        Input string in snake_case

    Returns
    -------
    str
        a normal string

    """
    return stringcase.sentencecase(s).lower()

def sentencecase_to_snakecase(s: str) -> str:
    """
    Convert sentence case to snake_case.

    Parameters
    ----------
    s: str
        Input string in sentence case

    Returns
    -------
    str
        a normal string

    """
    return stringcase.snakecase(s).lower()

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

def generate_edge_key(s: str, edge_label: str, o: str) -> str:
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

def get_biolink_mapping(category):
    global toolkit
    element = toolkit.get_element(category)
    if element is None:
        element = toolkit.get_element(snakecase_to_sentencecase(category))
    return element
