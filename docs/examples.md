# Examples scripts


## SemMedDB Knowledge Graph


### 1. Get all genomic entities that interacts with one or more genomic entities


```
python examples/scripts/read_from_neo4j.py --uri bolt://localhost:7687 \
    --username <username> \
    --password <password> \
    --filter subject_category=genomic_entity \
    --filter edge_label=interacts_with \
    --filter object_category=genomic_entity
```


- Total number of nodes loaded: 20656
- Total number of edges loaded: 171811


### 2. Get all chemical substances and what diseases they treat

```
python examples/scripts/read_from_neo4j.py --uri bolt://localhost:7687 \
    --username <username> \
    --password <password> \
    --filter subject_category=chemical_substance \
    --filter edge_label=treats \
    --filter object_category=disease

```


- Total number of nodes loaded: 49713
- Total number of edges loaded: 516334


### 3. Get all chemical substances and the biological process that they affect

```
python examples/scripts/read_from_neo4j.py --uri bolt://localhost:7687 \
    --username <username> \
    --password <password> \
    --filter subject_category=chemical_substance \
    --filter edge_label=affects \
    --filter object_category=biological_process
```

- Total number of nodes loaded: 43587
- Total number of edges loaded: 658355

### 4. Get all anatomical entities and the disease that they are associated with

```
python examples/scripts/read_from_neo4j.py --uri bolt://localhost:7687 \
    --username <username> \
    --password <password> \
    --filter subject_category=anatomical_entity \
    --filter edge_label=location_of \
    --filter object_category=disease
```

- Total number of nodes loaded: 36179
- Total number of edges loaded: 510709


### 5. Get all activities and behaviors and the diseases that they predispose to

```
python examples/scripts/read_from_neo4j.py --uri bolt://localhost:7687 \
    --username <username> \
    --password <password> \
    --filter subject_category=activity_and_behavior \
    --filter edge_label=predisposes \
    --filter object_category=disease
```

- Total number of nodes loaded: 1960
- Total number of edges loaded: 5493


### 6. Get all chemical substances and the diseases that they cause

```

python examples/scripts/read_from_neo4j.py --uri bolt://localhost:7687 \
    --username neo4j \
    --password <password> \
    --filter subject_category=chemical_substance \
    --filter edge_label=causes \
    --filter object_category=disease
```

- Total number of nodes loaded: 37977
- Total number of edges loaded: 299806


