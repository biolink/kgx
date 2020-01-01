"""
A module of useful methods used across the transformers
"""

import os

import click


def make_path(path) -> None:
    """
    Ensures that the directory structure exists
    """
    directory = os.path.dirname(path)
    if directory != '' and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def file_write(path:str, content:str, mode:str='w+', writeln:bool=True) -> None:
    """
    Writes content to file. Creates file path if directories do not yet exist.

    Parameters
    ----------
    path : The file path to save to. If directories do not yet exist they will be created
    content : A string to be saved to file
    mode : The mode with which to open the file
    writeln : Wheher or not to ensure that the content ends with a newline character
    """
    if writeln and not content.endswith('\n'):
        content += '\n'
    with click.open_file(path, mode=mode) as f:
        f.write(content)
