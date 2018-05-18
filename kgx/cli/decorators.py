import click

from kgx.cli.utils import Config

"""
May not be an excellent idea. This will change the signature of the wrapped
methods.

Is Click using the signature of the wrapped method?
"""

class handle_exception(object):
    """
    A decorator that catches exceptions and decides how to handle them
    """
    def __init__(self, f):
        self.f = f;
        self.__name__ = f.__name__
        self.__doc__ = f.__doc__
    def __call__(self, *args, **kwargs):
        try:
            return self.f(*args, **kwargs)

        except Exception as e:
            config = next((x for x in args if isinstance(x, Config)), None)

            if config is None:
                config = kwargs.get('config', None)

            if config is None:
                raise e;
            else:
                raise e if config.debug else click.ClickException(e)
