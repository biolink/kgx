import codecs
from typing import Generator, Dict, List, Any, Set, Optional, Union
from sys import stderr

from rdflib.plugins.parsers.ntriples import W3CNTriplesParser, ParseError
from rdflib.plugins.parsers.ntriples import r_wspace, r_wspaces, r_tail

from kgx.config import get_logger
from kgx.prefix_manager import PrefixManager
from kgx.utils.kgx_utils import generate_edge_key

logger = get_logger()


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
        self.prefix_manager = PrefixManager()
    
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

    _node_slots: Dict[str, Dict] = {
        "biolink:category": ("category", List),
        "rdf:type": ("type", str),  # TODO: not sure if rdf:type can be multivalued in the Biolink Model
        "biolink:name": ("name", str),
        "biolink:description": ("description", str),
        "biolink:provided_by": ("provided_by", List)
    }

    # _association_slots: Dict[str, Dict] = { ...? }  # see below...

    _node_cache: Dict[str, Dict[str, Union[str, List]]] = dict()

    @staticmethod
    def _annotate_node(node_id: str, slot_id: str, slot_value: Any):
        # This method aggregates node slot values
        # into a single entry in a dictionary cache
        if node_id not in CustomNTriplesParser._node_cache:
            CustomNTriplesParser._node_cache[node_id] = {"id": node_id}

        slot_name = CustomNTriplesParser._node_slots[slot_id][0]

        if CustomNTriplesParser._node_slots[slot_id][1] is List:
            # multivalued slot
            if slot_name not in CustomNTriplesParser._node_cache[node_id]:
                CustomNTriplesParser._node_cache[node_id][slot_name] = []

            if slot_value not in CustomNTriplesParser._node_cache[node_id][slot_name]:
                CustomNTriplesParser._node_cache[node_id][slot_name].append(slot_value)
        else:
            CustomNTriplesParser._node_cache[node_id][slot_name] = slot_value

        print(f"_annotate_node(): {str(CustomNTriplesParser._node_cache[node_id])}", file=stderr, flush=True)

    @staticmethod
    def _get_node(node_id: str) -> Optional[List]:
        # Return a given entry from the node dictionary cache
        if node_id in CustomNTriplesParser._node_cache:
            # Node record format: [id: str, node_data: Dict]
            return [node_id, CustomNTriplesParser._node_cache[node_id]]
        else:
            return None

    # cache of SPO assertions already set (based on their key identifier)
    _triple_cache: Set[str] = set()

    @staticmethod
    def _triple(subject, predicate, object) -> Optional[List]:
        # This method only returns a given (simple SPO) RDF statement once
        key = generate_edge_key(subject, predicate, object)
        if key not in CustomNTriplesParser._triple_cache:
            CustomNTriplesParser._triple_cache.add(key)
            # Edge record format: [subject: str, object: str, key: str, edge_data: Dict]
            # where edge_data is a dictionary of edge slot values
            return [
                subject,
                object,
                key,
                {
                    "id": key,
                    "subject": subject,
                    "predicate": predicate,
                    "object": object
                }
            ]
        else:
            return None  # we only return a triple once (bug or feature?)

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
            subject = str(self.prefix_manager.contract(subject))
            
            self.eat(r_wspaces)

            predicate = self.predicate()
            predicate = str(self.prefix_manager.contract(predicate))

            self.eat(r_wspaces)

            object = self.object()
            object = str(self.prefix_manager.contract(object))

            self.eat(r_tail)

            if self.line:
                raise ParseError("Trailing garbage")

            if predicate in CustomNTriplesParser._node_slots:

                CustomNTriplesParser._annotate_node(
                    node_id=subject,
                    slot_id=predicate,
                    slot_value=object
                )

                if self.current_node is None:
                    self.current_node = subject

                if subject == self.current_node:
                    # node properties assumed incomplete until you
                    # see another node; don't send it back just yet
                    return None

                else:
                    node_to_emit = self.current_node
                    self.current_node = subject
                    return CustomNTriplesParser._get_node(node_to_emit)

            # TODO: maybe also need to capture predicate statements here, that are association slots?

            else:
                # Emits a single edge record from ntriple RDF statement
                # which is not a simple node annotation;
                # Only sends the triple back once (None otherwise)
                return CustomNTriplesParser._triple(subject, predicate, object)
