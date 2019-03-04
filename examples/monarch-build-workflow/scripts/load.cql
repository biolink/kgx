CREATE CONSTRAINT ON (n:`named thing`) ASSERT n.id IS UNIQUE;

// +-------------------+
// | No data returned. |
// +-------------------+
// <Nothing happened>23 ms

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///clique_merged_nodes.csv" AS row
MERGE (n:`named thing` {id: row.id})
ON CREATE SET
  n.name = row.name,
  n.description = row.description,
  n.in_taxon = row.in_taxon,
  n.iri = row.iri,
  n.category = split(row.category, '|'),
  n.provided_by = split(row.provided_by, '|'),
  n.same_as = split(row.same_as, '|'),
  n.synonym = split(row.synonym, '|')
WITH *
CALL apoc.create.addLabels(n, coalesce(split(row.category, '|'), [])) YIELD node
RETURN COUNT(*);

// +----------+
// | COUNT(*) |
// +----------+
// | 526188   |
// +----------+
// 1 row
// Nodes created: 526188
// Properties set: 3696150
// Labels added: 526188
// 86518 ms

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM "file:///clique_merged_edges.csv" AS row
CALL apoc.map.fromPairs([k IN keys(row) WHERE NOT row[k] IS NULL | (CASE k WHEN 'publications' THEN [k, split(row[k], '|')] WHEN 'provided_by' THEN [k, split(row[k], '|')] ELSE [k, row[k]] END)]) YIELD value
MATCH (subject:`named thing` {id: row.subject})
MATCH (object:`named thing` {id: row.object})
CALL apoc.create.relationship(subject, value.edge_label, value, object) YIELD rel
RETURN COUNT(*);

// Without SemMedDb:
// +----------+
// | COUNT(*) |
// +----------+
// | 921075   |
// +----------+
// 1 row
// 143075 ms

// With SemMedDb:
// +----------+
// | COUNT(*) |
// +----------+
// | 921075   |
// +----------+
// 1 row
// 163999 ms
