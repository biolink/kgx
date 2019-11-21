from typing import Dict

import prefixcommons.curie_util as cu


class PrefixManager(object):
    """
    Manages prefix mappings.

    These include mappings for CURIEs such as GO:0008150, as well as shortforms such as
    biolink types such as Disease
    """

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
        if url is None:
            url = "https://raw.githubusercontent.com/biolink/biolink-model/master/context.jsonld"

        # NOTE: this is cached
        self.set_prefix_map(cu.read_remote_jsonld_context(url))

    def set_prefix_map(self, m: Dict) -> None:
        """
        Populate `prefix_map` with contents from a JSON-LD context from self.url

        Parameters
        ----------
        m: dict
            Dictionary of prefix to URI mappings

        """
        self.prefix_map = m
        self.reverse_prefix_map = {y: x for x, y in m.items() if isinstance(y, str)}

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
        uri = None
        if curie in self.prefix_map:
            uri = self.prefix_map[curie]
            # TODO: prefixcommons.curie_util will not unfold objects in json-ld context
            if isinstance(uri, str):
                return uri
        else:
            uri = cu.expand_uri(curie, [self.prefix_map])
            if uri == curie and fallback:
                uri = cu.expand_uri(curie)
        print("CURIE {} to IRI {}".format(curie, uri))
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
        curie = None
        print(uri)
        if uri in self.reverse_prefix_map:
            curie = self.reverse_prefix_map[uri]
        else:
            curie_list = cu.contract_uri(uri, [self.prefix_map])
            print(curie_list)
            if len(curie_list) == 0 and fallback:
                curie_list = cu.contract_uri(uri)
                if len(curie_list) != 0:
                    curie = curie_list[0]
            else:
                curie = curie_list[0]
        print("IRI {} to CURIE {}".format(uri, curie))
        return curie
