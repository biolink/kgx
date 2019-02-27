from kgx import ObanRdfTransformer, PandasTransformer
import networkx as nx
import pandas as pd

def load_edges(g:nx.Graph):
    """
    http://34.229.55.225/edges_neo4j.csv

    CSV row example:

    SEMMED_PRED,pmids,negated,:TYPE,:START_ID,:END_ID,n_pmids,is_defined_by,relation,provided_by
    AFFECTS,20801151,False,affects,UMLS:C1412045,UMLS:C0023946,1,semmeddb,semmeddb:affects,semmeddb_sulab
    """

    df = pd.read_csv('data/semmeddb_edges.csv')

    def process_row(row):
        p = row['pmids']
        p = ['PMID:' + i for i in p.split(';')] if p is not None else None

        kwargs = dict(
                semmedPredicate=row['SEMMED_PRED'],
                pmids=p,
                n_pmids=row['n_pmids'],
                negated=row['negated'],
                predicate=row[':TYPE'],
                defined_by=row['is_defined_by'],
                provided_by=row['provided_by'],
                relation=row['relation']
        )

        g.add_edge(row[':START_ID'], row[':END_ID'], **kwargs)


    df.apply(process_row, axis=1)

def load_nodes(g:nx.Graph):
    """
    Transforms the semmeddb node set into the required form, cleaning up xrefs (removing "NOCODE") members.

    CSV row example:

    :ID,name:STRING,umls_type:STRING[],umls_type_label:STRING[],:LABEL,xrefs:STRING[],category:STRING,id:STRING
    UMLS:C0061133,gastrin releasing peptide (14-27),T116,"Amino Acid, Peptide, or Protein",protein,MESH:C041922,protein,UMLS:C0061133
    """

    df = pd.read_csv('data/semmeddb_nodes.csv')

    def process_row(row):
        xrefs = row['xrefs:STRING[]']
        xrefs = [xref for xref in xrefs.split(';') if 'NOCODE' not in xref]


        kwargs = dict(
                name=row['name:STRING'],
                type=row['umls_type:STRING[]'],
                umls_type=row['umls_type_label:STRING[]'],
                label=row[':LABEL'],
                same_as=xrefs,
                category=row['category:STRING'],
                id=row['id:STRING']
        )

        n = row[':ID']

        if n in g:
            for key, value in kwargs.items():
                g.node[n][key] = value
        else:
            g.add_node(n, **kwargs)

    df.apply(process_row, axis=1)


if __name__ == '__main__':
    g = nx.Graph()
    t = PandasTransformer()
    load_nodes(t.graph)
    load_edges(t.graph)
    t.save('semmeddb.csv')
