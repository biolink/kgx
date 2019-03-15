from typing import Optional

def fmt_edgelabel(s:str) -> Optional[str]:
    """
    Returns the given string in snake case with commas removed
    """
    if isinstance(s, str):
        return s.replace(',', '').replace(' ', '_').lower()

def fmt_category(s:str) -> Optional[str]:
    if isinstance(s, str):
        return s.replace('_', ' ').lower()

def prefix(curie:str) -> Optional[str]:
    if ':' in curie:
        return curie.split(':', 1)[0]
