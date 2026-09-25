# Released under MIT License.
# Copyright (c) 2025-2026 Ladislav Bartos and Robert Vacha Lab


import re

from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)

_UNKNOWN_OBJECT_PATTERNS = (
    # qstat: Unknown queue gpu1234
    # qstat: Unknown Job Id 100.robox-pro.ceitec.muni.cz
    re.compile(r"^\s*\w+:\s+Unknown\s+(?:Job Id|queue|node)\s+(\S+)\s*$"),
    # Node: node.ceitec.muni.cz,  Error: Unknown node
    re.compile(r"^\s*Node:\s*([^,\s]+)\s*,\s*Error:\s*Unknown node\s*$"),
)


def _parse_unknown_pbs_object(line: str) -> str | None:
    """
    Extract the identifier from a PBS line reporting an unknown job, queue or node.

    Args:
        line (str): A single line of a PBS dump.

    Returns:
        str | None: Identifier of the unknown PBS object, or `None` if the line does
            not report an unknown object or carries no identifier.
    """
    for pattern in _UNKNOWN_OBJECT_PATTERNS:
        match = pattern.match(line)
        if match:
            return match.group(1)
    return None


def parse_pbs_dump_to_dictionary(text: str) -> dict[str, str]:
    """
    Parse a PBS info dump into a dictionary.

    Returns:
        dict[str, str]: Dictionary mapping keys to values.
    """
    result: dict[str, str] = {}

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        if " = " not in line:
            continue

        key, value = line.split(" = ", 1)
        result[key.strip()] = value.strip()

    return result


def parse_multi_pbs_dump_to_dictionaries(
    text: str, keyword: str | None
) -> list[tuple[dict[str, str], str]]:
    """
    Parse a PBS dump containing metadata for multiple queues/jobs/nodes into structured dictionaries.

    Args:
        text (str): The raw PBS dump containing information about one or more queues/jobs/nodes.
        keyword (str | None): Keyword identifying the start of a metadata block.
            If `None`, the first line is treated as identifier.

    Returns:
        list[tuple[dict[str, str], str]]: A list of tuples, each containing:
            - dict[str, str]: Parsed metadata for a single PBS object (job/queue/node).
            - str: Identifier (job ID / queue name / node name) extracted from the metadata.

    Raises:
        QQError: If the identifier cannot be extracted.
    """
    if not text.strip():
        return []

    data = []
    block, identifier = [], None
    pattern = pattern = (
        re.compile(rf"^\s*(?:.*?{keyword}[:\s]+)(.*)$") if keyword else None
    )

    for line in text.splitlines():
        # lines reporting unknown objects are parsed into empty dictionaries
        unknown = _parse_unknown_pbs_object(line)
        if unknown is not None:
            if block:
                data.append(
                    (parse_pbs_dump_to_dictionary("\n".join(block)), identifier)
                )
                block, identifier = [], None
            data.append(({}, unknown))
            continue

        # if the line is empty, start a new block
        if not line.strip():
            if block:
                data.append(
                    (parse_pbs_dump_to_dictionary("\n".join(block)), identifier)
                )
                block, identifier = [], None
            continue

        if not block:
            # extract the identifier
            if pattern:
                m = pattern.match(line)
                if not m:
                    raise QQError(
                        f"Invalid PBS dump format. Could not extract identifier from:\n{line}"
                    )
                identifier = m.group(1).strip()
            # if keyword is not specified, use the first line as the identifier
            else:
                identifier = line.strip()
        block.append(line)

    # last block (no trailing newline)
    if block:
        data.append((parse_pbs_dump_to_dictionary("\n".join(block)), identifier))

    logger.debug(f"Detected and parsed metadata for {len(data)} PBS objects.")
    return data
