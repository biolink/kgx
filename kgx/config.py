import logging
import sys
from os import path
import json
from typing import Dict, Any, Optional

import requests
import yaml

config: Optional[Dict[str, Any]] = None
logger: Optional[logging.Logger] = None
jsonld_context_map: Dict = {}

CONFIG_FILENAME = path.join(path.dirname(path.abspath(__file__)), 'config.yml')


def get_config(filename: str = CONFIG_FILENAME) -> dict:
    """
    Get config as a dictionary

    Parameters
    ----------
    filename: str
        The filename with all the configuration

    Returns
    -------
    dict
        A dictionary containing all the entries from the config YAML

    """
    global config
    if config is None:
        config = yaml.load(open(filename), Loader=yaml.FullLoader)
    return config


def get_jsonld_context(name: str = "biolink"):
    """
    Get contents of a JSON-LD context.

    Returns
    -------
    dict
        the contents of the JSON-LD context

    """
    content = None
    if name in jsonld_context_map:
        content = jsonld_context_map[name]
    else:
        filepath = config['jsonld-context'][name] # type: ignore
        if filepath.startswith('http'):
            try:
                content = requests.get(filepath).json()
            except ConnectionError:
                raise Exception(f'Unable to download JSON-LD context from {filepath}')
        else:
            if path.exists(filepath):
                content = json.load(open(filepath))

        if '@context' in content:
            content = content['@context']
            jsonld_context_map[name] = content
    return content


def get_logger(name: str = 'KGX') -> logging.Logger:
    """
    Get an instance of logger.

    Parameters
    ----------
    name: str
        The name of logger

    Returns
    -------
    logging.Logger
        An instance of logging.Logger

    """
    global logger
    if logger is None:
        config = get_config()
        logger = logging.getLogger(name)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(config['logging']['format'])
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(config['logging']['level'])
        logger.propagate = False
    return logger
