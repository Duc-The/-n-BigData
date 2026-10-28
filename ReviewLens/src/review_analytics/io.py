"""Publish small local artifacts atomically across Spark and Airflow users."""
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile


@contextmanager
def atomic_text_writer(path: Path, encoding="utf-8", newline=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding=encoding, newline=newline) as handle:
            yield handle
        # Containers share group 0. A new artifact remains writable by both users.
        os.chmod(temporary, 0o664)
        # Replacement needs permission on the directory, not on the old file.
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_text(path: Path, content: str):
    with atomic_text_writer(path) as handle:
        handle.write(content)

