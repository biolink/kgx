import logging
import re
from typing import Dict

import prefixcommons.curie_util as cu

from kgx.config import get_jsonld_context
from kgx.utils.kgx_utils import contract, expand


class PrefixManager(object):
    """
    Manages prefix mappings.

    These include mappings for CURIEs such as GO:0008150, as well as shortforms such as
    biolink types such as Disease
    """
    DEFAULT_NAMESPACE = 'https://www.example.org/UNKNOWN/'
    prefix_map = None
    reverse_prefix_map = None

    def __init__(self, url: str = None):
        """
        Initialize an instance of PrefixManager.

        Parameters
        ----------
        url: str
            The URL from which to read a JSON-LD context for prefix mappings

        """
        if url:
            context = cu.read_remote_jsonld_context(url)
        else:
            context = get_jsonld_context()
        self.set_prefix_map(context)

    def set_prefix_map(self, m: Dict) -> None:
        """
        Populate `prefix_map` with contents from a JSON-LD context from self.url

        Parameters
        ----------
        m: dict
            Dictionary of prefix to URI mappings

        """
        self.prefix_map = {}
        for k, v in m.items():
            if isinstance(v, str):
                self.prefix_map[k] = v
        if 'biolink' not in self.prefix_map:
            self.prefix_map['biolink'] = self.prefix_map['@vocab']
            del self.prefix_map['@vocab']
        if ':' in self.prefix_map:
            logging.info(f"Replacing default prefix mapping from {self.prefix_map[':']} to 'www.example.org/UNKNOWN/'")
        else:
            self.prefix_map[':'] = self.DEFAULT_NAMESPACE

        self.reverse_prefix_map = {y: x for x, y in self.prefix_map.items()}

    def expand(self, curie: str, fallback: bool = True) -> str:
        """
        Expand a given CURIE to an URI, based on mappings from `prefix_map`.

        Parameters
        ----------
        curie: str
            A CURIE
        fallback: bool
            Determines whether to fallback to default prefix mappings, as determined
            by `prefixcommons.curie_util`, when CURIE prefix is not found in `prefix_map`.

        Returns
        -------
        str
            A URI corresponding to the CURIE

        """
        uri = expand(curie, [self.prefix_map], fallback)
        return uri

    def contract(self, uri: str, fallback: bool = True) -> str:
        """
        Contract a given URI to a CURIE, based on mappings from `prefix_map`.

        Parameters
        ----------
        uri: str
            A URI

        fallback: bool
            Determines whether to fallback to default prefix mappings, as determined
            by `prefixcommons.curie_util`, when URI prefix is not found in `reverse_prefix_map`.

        Returns
        -------
        str
            A CURIE corresponding to the URI

        """
        # always prioritize non-CURIE shortform
        if uri in self.reverse_prefix_map:
            curie = self.reverse_prefix_map[uri]
        else:
            curie = contract(uri, [self.prefix_map], fallback)
        return str(curie)

    @staticmethod
    def is_curie(s: str) -> bool:
        """
        Check if a given string is a CURIE.

        Parameters
        ----------
        s: str
            A string

        Returns
        -------
        bool
            Whether or not the given string is a CURIE

        """
        if isinstance(s, str):
            m = re.match(r"^[^ <()>:]*:[^/ :]+$", s)
            return bool(m)
        else:
            return False

    @staticmethod
    def is_iri(s: str) -> bool:
        """
        Check if a given string as an IRI.

        Parameters
        ----------
        s: str
            A string

        Returns
        -------
        bool
            Whether or not the given string is an IRI.

        """
        if isinstance(s, str):
            return s.startswith('http') or s.startswith('https')
        else:
            return False

    @staticmethod
    def get_prefix(curie: str) -> str:
        """
        Get the prefix from a given CURIE.

        Parameters
        ----------
        curie: str
            The CURIE

        Returns
        -------
        str
            The CURIE prefix

        """
        prefix = None
        if PrefixManager.is_curie(curie):
            prefix = curie.split(':', 1)[0]
        return prefix

    @staticmethod
    def get_reference(curie: str) -> str:
        """
        Get the reference of a given CURIE.

        Parameters
        ----------
        curie: str
            The CURIE

        Returns
        -------
        str
            The reference of a CURIE

        """
        reference = None
        if PrefixManager.is_curie(curie):
            reference = curie.split(':', 1)[1]
        return reference
