"""Resolve literal paths, pyseq references, and shell-style wildcards."""

import glob
import os
from typing import Iterable, List, Union

from pyseq.util import (
    is_compressed_format_string,
    parse_explicit_sequence_string,
    resolve_sequence_reference,
)


def resolve_inputs(references: Iterable[Union[str, os.PathLike]]) -> List[str]:
    """Return unique absolute paths, giving existing literal paths precedence.

    Explicit sequence ranges must exist in full. Patterns without a range
    select the frames currently present on disk.

    :param references: literal file/directory paths, wildcards, or pyseq references
    :returns: unique absolute paths in selection order
    :raises ValueError: if an input is invalid, missing, or has no matches
    """
    paths = []
    seen = set()
    for reference in references:
        reference = os.fspath(reference)
        try:
            if os.path.exists(reference):
                matches = [reference]
            elif is_compressed_format_string(os.path.basename(reference)) or (
                parse_explicit_sequence_string(reference) is not None
            ):
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
            for path in matches:
                path = os.path.abspath(path)
                if not (os.path.isfile(path) or os.path.isdir(path)):
                    raise ValueError("Not a regular file or directory: {}".format(path))
                key = os.path.normcase(path)
                if key not in seen:
                    seen.add(key)
                    paths.append(path)
        except (OSError, ValueError) as error:
            raise ValueError("{}: {}".format(reference, error)) from error
    return paths
