import codecs
from typing import Generator, Dict, List, Any, Set, Optional, Union, Tuple

from rdflib.plugins.parsers.ntriples import W3CNTriplesParser, ParseError
from rdflib.plugins.parsers.ntriples import r_wspace, r_wspaces, r_tail

from kgx.config import get_logger
from kgx.prefix_manager import PrefixManager
from kgx.utils.kgx_utils import generate_edge_key, get_toolkit

logger = get_logger()

bmt = get_toolkit()
pm = PrefixManager()


class CustomNTriplesParser(W3CNTriplesParser):
    """
    This class is an extension to ``rdflib.plugins.parsers.ntriples.W3CNTriplesParser``
    that parses N-Triples and yields triples.
    """

    def __init__(self, sink=None):
        W3CNTriplesParser.__init__(self, sink=sink)
        self.file = None
        self.buffer = ""
        self.line = ""
        self.current_node: Optional[str] = None
        self.current_reification: Optional[str] = None

    # Set of Biolink Associations, based on their S-P-O key identifier
    _triple_cache: Set[str] = set()

    @staticmethod
    def _process_association(
            subject: str,
            predicate: Optional[str] = None,
            object: Optional[str] = None,
            record: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> Optional[List]:

        # This method only publishes a given RDF statement once
        key = generate_edge_key(subject, predicate, object)

        # We only publish a given association once unless it is
        # encountered more than once as a reified node
        if record or key not in CustomNTriplesParser._triple_cache:
            if record:
                # overwrite (with key) the blank node urn: id of the reified
                # association (as signaled by a non-null record)
                record["id"] = key
            else:
                # Likely a direct assertion (NOT derived from a
                # reified node, with its additional slot values)
                # so just add the basic record fields
                record = {
                    "id": key,
                    "subject": subject,
                    "predicate": predicate,
                    "object": object
                }

            CustomNTriplesParser._triple_cache.add(key)  # this tags the key as seen already

            # Edge record format: [subject: str, object: str, key: str, edge_data: Dict]
            # where edge_data is a dictionary of edge slot values
            return [
                subject,
                object,
                key,
                record  # embedded data dictionary of the Association slots
            ]

        return None  # we only return a triple once (bug or feature?)

    # cache of concept node slot data
    _node_cache: Dict[str, Dict[str, Union[str, List]]] = dict()

    # cache of reified association node slot data
    _reification_cache: Dict[str, Dict[str, Union[str, List[str]]]] = dict()

    def flush_current_entity(self) -> Optional[List]:

        # Returns: KGX nodes and edge record data, for transformer.process() access

        if self.current_node:

            # flush the most recently parsed concept node

            node_to_emit: Optional[List] = None

            if self.current_node in CustomNTriplesParser._node_cache:
                # Node record format: [id: str, node_data: Dict]
                node_to_emit = [self.current_node, CustomNTriplesParser._node_cache[self.current_node]]
            else:
                logger.error(f"_entity_to_emit(): this is strange: node '{self.current_node}' not found in node cache?")

            self.current_node = None

            return node_to_emit

        if self.current_reification:

            # flush the most recently parsed reified association

            association_to_emit: Optional[List] = None

            if self.current_reification in CustomNTriplesParser._reification_cache:

                # we substitute the reification node with its Biolink Association, since
                # we've been collecting slot values slots for a reified association
                record = CustomNTriplesParser._reification_cache[self.current_reification]

                # Need a minimally well-formed Association here, unless the NT collection was incomplete?
                try:
                    # core S-P-O slots
                    subject = record["subject"]
                    predicate = record["predicate"]
                    object = record["object"]

                    # Assumed complete (for now) so we'll remove it from the cache now and publish it
                    # TODO: this may be problematic if all of the RDF statements describing the reified association
                    #       are not contiguous to one another, and the core S-P-O slots are not duplicated where needed
                    CustomNTriplesParser._reification_cache.pop(self.current_reification)

                    association_to_emit = CustomNTriplesParser._process_association(subject, predicate, object, record)

                    self.current_reification = None

                except KeyError:
                    logger.warning(f"_reified_association() incomplete minimal triple: '{str(record)}'?")

            return association_to_emit

        # If we fall through here, then the caches are empty?")
        return None

    def parse(self, f, bnode_context=None) -> Generator:
        """
        Parses an N-Triples file and yields triples.

        Parameters
        ----------
        f:
            The file-like object to parse
        bnode_context:
            a dict mapping blank node identifiers (e.g., ``a`` in ``_:a``)
            to `~rdflib.term.BNode` instances. An empty dict can be
            passed in to define a distinct context for a given call to `parse`.

        Returns
        -------
        Generator
            A generator for triples

        """
        if not hasattr(f, "read"):
            raise ParseError("Item to parse must be a file-like object.")

        # since N-Triples 1.1 files can and should be utf-8 encoded
        f = codecs.getreader("utf-8")(f)

        self.file = f
        self.buffer = ""
        while True:
            self.line = self.readline()
            if self.line is None:
                break
            if self.line == "":
                raise ParseError(f"Empty line encountered in {str(f)}. "
                                 f"Ensure that no leading or trailing empty lines persist "
                                 f"in the N-Triples file.")
            try:
                # parseline internally caches information from the RDF and only emits a
                # Biolink Entity - concept node or statement edge - when complete
                # (and tries to only emit such entities once?). Returns None otherwise
                entity = self.parseline()
                if entity:
                    yield entity

            except ParseError:
                raise ParseError("Invalid line: %r" % self.line)

        # after the EOF is encountered, publish the more
        # recent Biolink Model entities that we encountered
        # TODO: there may be a subtle node ordering problem here, in that a dangling node may
        #       potentially not be published prior to being used in another RDF statement?
        yield self.flush_current_entity()

    _node_slots: Dict[str, Tuple[str, Union[str, List]]] = {
        "biolink:category": ("category", List),
        "rdf:type": ("type", str),  # TODO: not sure if rdf:type can be multivalued in the Biolink Model
        "biolink:name": ("name", str),
        "biolink:description": ("description", str),
        "biolink:provided_by": ("provided_by", List),
    }

    _association_slots: Dict[str, Tuple[str, Union[str, List]]] = {
        "biolink:category": ("category", List),
        # _association slots are typically specified in RDF as a
        #  reification using a local(i.e. blank) node RDF subject resource
        "rdf:type": ("type", str),  # an association 'type' will be 'biolink:Association' or a child class thereof
        "biolink:subject": ("subject", str),
        "biolink:predicate": ("predicate", str),
        "biolink:object": ("object", str),
        "biolink:edge_label": ("predicate", str),  # edge_label deprecated in Biolink Model 1.4.0; removed in 2.0.0
        "biolink:relation": ("relation", str),     # deprecated in Biolink Model 2.3.0, but we still processing it here
        "biolink:knowledge_source": ("knowledge_source", List),
        "biolink:primary_knowledge_source": ("primary_knowledge_source", str),
        "biolink:original_knowledge_source": ("original_knowledge_source", str),
        "biolink:aggregator_knowledge_source": ("aggregator_knowledge_source", List),
        "biolink:supporting_data_source": ("supporting_data_source", List),
    }

    _deprecated_slots: Dict[str, str] = {
        "edge_label": "1.4.0",
        "relation": "2.3.0",
        "provided_by": "2.2.0"
    }

    @staticmethod
    def _capture_slot(
            node_id: str,
            slot_id: str,
            slot_value: Any,
            slot_map: Dict[str, Tuple[str, Union[str, List]]],
            slot_cache: Dict
    ):
        # Shared code for node and reified association slot annotation methods
        if node_id not in slot_cache:
            slot_cache[node_id] = {"id": node_id}

        if slot_id in slot_map:

            if slot_id in CustomNTriplesParser._deprecated_slots:
                dep_release = CustomNTriplesParser._deprecated_slots[slot_id]
                logger.warning(
                    f"Biolink Model slot '{slot_id}' is deprecated " +
                    f"since Biolink Model Release '{dep_release}'.")
                # TODO: one may even completely ignore a slot if the current
                #       Biolink Model release used here by BMT doesn't have it?

            # slot_id is an expected core slot
            slot_name = slot_map[slot_id][0]

            if slot_map[slot_id][1] is List:
                # multivalued slot
                if slot_name not in slot_cache[node_id]:
                    slot_cache[node_id][slot_name] = []

                if slot_value not in slot_cache[node_id][slot_name]:
                    slot_cache[node_id][slot_name].append(slot_value)
            else:
                slot_cache[node_id][slot_name] = slot_value
        else:
            # not a regular slot; we won't throw an exception here forever, but rather just capture
            # the slot values "as is" as scalars, using the CURIE reference directly as the slot name
            # TODO: should BMT be used to validate such ad hoc slots here as Biolink Model Compliant?
            # TODO: should all of these rather be captured as _attribute JSON blobs here?
            slot_name = str(pm.get_reference(curie=slot_id))
            slot_cache[node_id][slot_name] = slot_value

    @staticmethod
    def _annotate_node(node_id: str, slot_id: str, slot_value: Any):
        # This method aggregates node slot values
        # into a single entry in a dictionary cache
        CustomNTriplesParser._capture_slot(
            node_id=node_id,
            slot_id=slot_id,
            slot_value=slot_value,
            slot_map=CustomNTriplesParser._node_slots,
            slot_cache=CustomNTriplesParser._node_cache
        )

    @staticmethod
    def _annotate_reified_association(node_id: str, slot_id: str, slot_value: Any):
        # This method aggregates association slot values
        # from node slot values of reified associations
        # into a single entry in a dictionary cache
        CustomNTriplesParser._capture_slot(
            node_id=node_id,
            slot_id=slot_id,
            slot_value=slot_value,
            slot_map=CustomNTriplesParser._association_slots,
            slot_cache=CustomNTriplesParser._reification_cache
        )

    def _entity_to_emit(self, node_id: str) -> Optional[List]:
        # This code assumes contiguity in RDF statements of properties
        # for a given reified association, bounded by a change in subject resource ID
        entity: Optional[List] = None

        if pm.is_curie(node_id):
            # Is an IRI => regular concept node
            if node_id != self.current_node:
                entity = self.flush_current_entity()
                self.current_node = node_id
        else:
            # Is a blank node URN => reification of an association
            if node_id != self.current_reification:
                entity = self.flush_current_entity()
                self.current_reification = node_id

        return entity

    def _process_node(
            self,
            subject: str,  # node_id
            predicate: Optional[str] = None,
            object: Optional[str] = None
    ) -> Optional[List]:

        # Capture node properties of a regular concept node
        CustomNTriplesParser._annotate_node(
            node_id=subject,
            slot_id=predicate,
            slot_value=object
        )

        # if subject node is 'new' then emit any previously parsed entity
        return self._entity_to_emit(subject)

    def _process_reified_association(
            self,
            subject: str,  # node_id of association reification node
            predicate: Optional[str] = None,
            object: Optional[str] = None
    ) -> Optional[List]:

        CustomNTriplesParser._annotate_reified_association(
            node_id=subject,
            slot_id=predicate,
            slot_value=object
        )

        # if subject reification node is 'new' then emit any previously parsed entity
        return self._entity_to_emit(subject)

    def parseline(self, bnode_context=None):
        """
        Parse each line and yield triples.

        Parameters
        ----------
        bnode_context:
            a dict mapping blank node identifiers (e.g., ``a`` in ``_:a``)
            to `~rdflib.term.BNode` instances. An empty dict can be
            passed in to define a distinct context for a given call to `parse`.
        """
        # TODO: How would we handle blank nodes here (using bnode_context)?
        if not hasattr(self, 'sink'):
            raise ParseError("CustomNTriplesParser is missing a sink?")

        self.eat(r_wspace)
        
        if self.line and not self.line.startswith("#"):
            
            subject = self.subject()
            subject = str(pm.contract(subject))
            
            self.eat(r_wspaces)

            predicate = self.predicate()
            predicate = str(pm.contract(predicate))

            predicate_by_mapping = bmt.get_element_by_mapping(predicate, formatted=True)
            if predicate_by_mapping:
                predicate = predicate_by_mapping

            self.eat(r_wspaces)

            object = self.object()
            object = str(pm.contract(object))

            self.eat(r_tail)

            if self.line:
                raise ParseError("Trailing garbage")

            # Distinct Biolink Model concept nodes and associations are emitted here:
            # Note that the three methods return 'None' if they discern that
            # the construction of the current entity is 'incomplete'
            # (i.e. that there are still expected to be additional node attributes
            # for a regular concept node or a node-reified association edge)
            if bmt.is_predicate(predicate):
                # If the predicate is a Biolink Model defined predicate (inheriting from 'related_to') then
                # emit a single edge record from a directly asserted (not reified) ntriple RDF S-P-O statement
                # (NOTE: the method only sends a triple back once (None otherwise))
                return CustomNTriplesParser._process_association(subject, predicate, object)

            elif pm.is_curie(subject):
                # Regular URI - not a blank node - treat as a regular concept node
                return self._process_node(subject, predicate, object)

            else:
                # blank node local urn: identifier - treat as a reified association
                return self._process_reified_association(subject, predicate, object)
