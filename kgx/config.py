from os import path
import json

import requests
import yaml

config = None

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
    url = config['jsonld-context'][name]['local']
    if path.exists(url):
        content = json.load(open(url))
    else:
        try:
            url = config['jsonld-context'][name]['remote']
            content = requests.get(url).json()
        except ConnectionError:
            raise Exception(f'Unable to download JSON-LD context from {url}')
    if '@context' in content:
        content = content['@context']
    return content
