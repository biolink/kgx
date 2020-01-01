import os
from collections import defaultdict
from typing import List

import click

from kgx.validator import Error, NodeError, EdgeError


def append_errors_to_file(filename:str, errors:List[Error], time) -> None:
    """
    Creates a single file that logs all errors
    """
    dirname = os.path.dirname(filename)
    if dirname != '':
        os.makedirs(dirname, exist_ok=True)

    with click.open_file(filename, 'a+') as f:
        f.write('--- {} ---\n'.format(time))

        for e in errors:
            if isinstance(e, NodeError):
                f.write('node({})\t{}\n'.format(e.node, e.message))
            elif isinstance(e, EdgeError):
                f.write('edge({}, {})\t{}\n'.format(e.subject, e.object, e.message))
            else:
                raise Exception('Expected type {} but got: {}'.format(Error, type(error)))

        click.echo('Logged {} errors to {}'.format(len(errors), filename))

def append_errors_to_files(dirname:str, errors:List[Error], time) -> None:
    """
    Creates a series of files that logs all errors, each error type being logged
    to its own file.
    """
    os.makedirs(dirname, exist_ok=True)

    error_dict = defaultdict(list)
    for error in errors:
        error_dict[error.error_type].append(error)

    for error_type, typed_errors in error_dict.items():
        if error_type is None:
            error_type = 'default.log'
        else:
            error_type = error_type.replace(' ', '_') + '.log'

        filename = os.path.join(dirname, error_type)
        with click.open_file(filename, 'a+') as f:
            f.write('--- {} ---\n'.format(time))
            for e in typed_errors:
                if isinstance(e, NodeError):
                    f.write('node({})\t{}\n'.format(e.node, e.message))
                elif isinstance(e, EdgeError):
                    f.write('edge({}, {})\t{}\n'.format(e.subject, e.object, e.message))
                else:
                    raise Exception('Expected type {} but got: {}'.format(Error, type(e)))

            click.echo('Logged {} errors to {}'.format(len(typed_errors), filename))
