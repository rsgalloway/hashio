"""Resolve literal paths, pyseq references, and shell-style wildcards."""

import glob
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Union

from pyseq.util import (
    is_compressed_format_string,
    parse_explicit_sequence_string,
    resolve_sequence_reference,
)


@dataclass
class InputSelection:
    """Resolved inputs with explicit sequences retained as logical groups.

    :param paths: unique absolute paths to scan
    :param sequences: original sequence references mapped to their frame paths
    :param files: explicitly selected non-sequence files for console output
    """

    paths: List[str]
    sequences: Dict[str, List[str]]
    files: List[str]


def resolve_selection(references: Iterable[Union[str, os.PathLike]]) -> InputSelection:
    """Resolve inputs while preserving explicit sequence groups.

    Explicit sequence ranges must exist in full. Patterns without a range
    select the frames currently present on disk.

    :param references: literal file/directory paths, wildcards, or pyseq references
    :returns: resolved paths, sequence groups, and ordinary file selections
    :raises ValueError: if an input is invalid, missing, or has no matches
    """
    paths = []
    sequences = {}
    files = []
    seen = set()
    for reference in references:
        reference = os.fspath(reference)
        try:
            is_sequence = False
            if os.path.exists(reference):
                matches = [reference]
            elif is_compressed_format_string(os.path.basename(reference)) or (
                parse_explicit_sequence_string(reference) is not None
            ):
                is_sequence = True
                sequence, directory = resolve_sequence_reference(reference)
                matches = [os.path.join(directory, item.name) for item in sequence]
                if not all(os.path.isfile(path) for path in matches):
                    raise FileNotFoundError(
                        "Sequence contains missing or non-file entries"
                    )
            elif glob.has_magic(reference):
                matches = sorted(glob.glob(reference))
            else:
                matches = []
            if not matches:
                raise FileNotFoundError("No matching files or directories")
            if is_sequence:
                sequences[reference] = [os.path.abspath(path) for path in matches]
            for path in matches:
                path = os.path.abspath(path)
                if not (os.path.isfile(path) or os.path.isdir(path)):
                    raise ValueError("Not a regular file or directory: {}".format(path))
                if not is_sequence and os.path.isfile(path):
                    files.append(path)
                key = os.path.normcase(path)
                if key not in seen:
                    seen.add(key)
                    paths.append(path)
        except (OSError, ValueError) as error:
            raise ValueError("{}: {}".format(reference, error)) from error
    return InputSelection(paths, sequences, files)


def resolve_inputs(references: Iterable[Union[str, os.PathLike]]) -> List[str]:
    """Return flattened paths for callers requesting per-file hashing.

    :param references: literal paths, wildcards, or pyseq references
    :returns: unique absolute paths in selection order
    :raises ValueError: if an input is invalid, missing, or has no matches
    """
    return resolve_selection(references).paths
