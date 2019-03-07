import click, yaml
from .source import Source
from neo4jrestclient.client import GraphDatabase as http_gdb

class NeoSource(Source):
    """
    We expect a Translator canonical style http://bit.ly/tr-kg-standard
    E.g. predicates are names with underscores, not IDs.

    Does not load from config file if uri and username are provided.
    """
    def __init__(self, sink, uri=None, username=None, password=None):
        super(NeoSource, self).__init__(sink)
        if uri is username is None:
            with open("config.yml", 'r') as ymlfile:
                cfg = yaml.load(ymlfile)
                uri = "{}:{}".format(cfg['neo4j']['host'], cfg['neo4j']['port'])
                username = cfg['neo4j']['username']
                password = cfg['neo4j']['password']
        self.driver = http_gdb(uri, username=username, password=password)

    def load(self):
        """
        Read a neo4j database and stream it to a sink
        """
        match = 'match (n{})-[e{}]->(m{})'
        click.echo('Using cyper query: {} return n, e, m'.format(match))

        results = self.driver.query('{} return count(*)'.format(match))
        for a, in results:
            size = a
            break
        if size == 0:
            click.echo('No data available')
            quit()

        skip_flag = False
        nodes_seen = set()
        page_size = 50000 #1_000
        with click.progressbar(list(range(0, size, page_size)), label='Downloading {} many edges'.format(size)) as bar:
            for i in bar:
                q = '{} return n, e, m skip {} limit {}'.format(match, i, page_size)
                results = self.driver.query(q)

                for n, e, m in results:
                    subject_attr = n['data']
                    object_attr = m['data']
                    edge_attr = e['data']

                    if 'id' not in subject_attr or 'id' not in object_attr:
                        if not skip_flag:
                            click.echo('Skipping records that have no id attribute')
                            skip_flag = True
                        continue

                    s = subject_attr['id']
                    o = object_attr['id']

                    if 'edge_label' not in edge_attr:
                        edge_attr['edge_label'] = e['metadata']['type']
                    if 'category' not in subject_attr:
                        subject_attr['category'] = n['metadata']['labels']
                    if 'category' not in object_attr:
                        object_attr['category'] = m['metadata']['labels']

                    if s not in nodes_seen:
                        self.sink.add_node(s, subject_attr)
                        nodes_seen.add(s)
                    if o not in nodes_seen:
                        self.sink.add_node(o, object_attr)
                        nodes_seen.add(o)
                    self.sink.add_edge(s, o, edge_attr)
