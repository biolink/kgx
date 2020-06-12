from os import path

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
