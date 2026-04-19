"""Hardened, streaming XML parsing for SDMX payloads.

Every parser we expose here has external entity resolution, DTD loading,
and network fetches disabled. ECB's ``?detail=nodata`` response can be
hundreds of MB, so reading is always done via ``iterparse`` with
per-element clearing **and root-level cleanup** to keep peak memory
bounded.

:func:`iter_elements` accepts ``bytes`` (in-memory payload) or a
file-like object with a ``read`` method (e.g., ``response.raw``) so very
large responses can be parsed without first materialising the whole body
in Python memory.
"""

from __future__ import annotations

from collections.abc import Iterator
from io import BytesIO
from typing import Any, BinaryIO

from lxml import etree

from parsimony_sdmx.core.errors import SdmxFetchError

_SAFE_PARSER_KWARGS: dict[str, Any] = {
    "resolve_entities": False,
    "no_network": True,
    "load_dtd": False,
    "dtd_validation": False,
    "huge_tree": False,
    "recover": False,
}

# Clear root-level children every N yields so an ancestor element's
# children-list doesn't accumulate empty element shells as we stream.
_ROOT_CLEAR_EVERY = 1000


def iter_elements(
    source: bytes | BinaryIO,
    tag: str,
) -> Iterator[Any]:
    """Stream elements matching ``tag`` using hardened ``iterparse``.

    ``tag`` may be a plain local name or a ``{namespace}localname``
    Clark-notation tag. After each yield, the element is ``clear()``-ed,
    its prior siblings are removed, and — to prevent the root and
    intermediate ancestors from accumulating empty element shells on
    very large payloads — the root element is cleared periodically.

    ``source`` may be ``bytes`` (wrapped in ``BytesIO``) or any
    file-like object with a ``read`` method. Pass ``response.raw``
    (after ``response.raw.decode_content = True``) to parse directly
    off the socket without buffering.

    **Callers must read all needed data inside the loop body.** Holding
    an element reference past its yield (for example via ``list(...)``)
    will surface cleared values, because the generator resumes and
    clears the element before the next iteration.

    Raises :class:`SdmxFetchError` on malformed XML.
    """
    stream: BinaryIO = BytesIO(source) if isinstance(source, (bytes, bytearray)) else source
    try:
        context = etree.iterparse(
            stream,
            events=("start", "end"),
            tag=tag,
            **_SAFE_PARSER_KWARGS,
        )
        root: Any | None = None
        yielded = 0
        for event, elem in context:
            if event == "start":
                if root is None:
                    # First start event: capture the top of the tree so we
                    # can periodically clear its children as we stream.
                    # For an element yielded directly under the root (the
                    # common SDMX shape), getparent() is None on the first
                    # yielded end-event, so we record it here instead.
                    ancestor = elem
                    parent = ancestor.getparent()
                    while parent is not None:
                        ancestor = parent
                        parent = ancestor.getparent()
                    root = ancestor
                continue
            # event == "end"
            yield elem
            yielded += 1
            elem.clear(keep_tail=False)
            parent = elem.getparent()
            if parent is not None:
                while elem.getprevious() is not None:
                    del parent[0]
            # Periodically clear the root to keep ancestor children lists flat.
            if root is not None and yielded % _ROOT_CLEAR_EVERY == 0:
                # Preserve the root's tag/attrs; drop children only.
                for child in list(root):
                    root.remove(child)
    except etree.XMLSyntaxError as exc:
        raise SdmxFetchError(f"Malformed XML: {exc}") from exc


def parse_xml(xml_bytes: bytes) -> Any:
    """Parse a small XML document into a root element with hardened settings.

    For bulk payloads prefer :func:`iter_elements`. This helper is for
    cases where the whole tree must be inspected at once (metadata
    responses, small structure messages).
    """
    parser = etree.XMLParser(**_SAFE_PARSER_KWARGS)
    try:
        return etree.fromstring(xml_bytes, parser)
    except etree.XMLSyntaxError as exc:
        raise SdmxFetchError(f"Malformed XML: {exc}") from exc
