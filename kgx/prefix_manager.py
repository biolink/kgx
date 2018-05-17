import prefixcommons.curie_util as cu

class PrefixManager(object):
    """
    Manages prefix mappings.

    These include mappings for CURIEs such as GO:0008150, as well as shortforms such as
    biolink types such as Disease
    """
    def __init__(self, url=None):
        if url is None:
            url = "https://raw.githubusercontent.com/biolink/biolink-model/master/context.jsonld"

        # NOTE: this is cached
        # to clear cache: rm ~/.cachier/.prefixcommons.curie_util.read_remote_jsonld_context 
        self.prefixmap = cu.read_remote_jsonld_context(url)

    def expand(self, id):
        uri = cu.expand_uri(id, [self.prefixmap])
        return uri

    def contract(self, uri):
        shortform = cu.contract_uri(uri, [self.prefixmap])
        return shortform
