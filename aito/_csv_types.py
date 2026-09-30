"""CSV parsing and column typing for ``Client.upload_csv``, the same rules as the engine

Mirrors aito-core #1535 (``CollectionDbCsvImport`` and ``DocumentType.StringStats``), so a
CSV uploaded through the client path makes the same collection as one the engine imports
itself. Standard library only: ``upload_csv`` works on a bare ``pip install aitoai``.
Design: ``docs/adr/0003-sdk-csv-upload.md``.
"""

import csv
import io
import os
import re
from typing import IO, Dict, List, Optional, Sequence, Tuple, Union

CsvSource = Union[str, 'os.PathLike[str]', bytes, IO]

_INT_RE = re.compile(r'^-?[0-9]+$')
_DECIMAL_RE = re.compile(r'^-?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$')
_LONG_MIN, _LONG_MAX = -2 ** 63, 2 ** 63 - 1
_INT_MIN, _INT_MAX = -2 ** 31, 2 ** 31 - 1
#: The engine stops counting distinct values here; the rule needs at most 21.
_DISTINCT_CAP = 64


class CsvFormatError(ValueError):
    """the CSV itself is malformed; the message names the row or column"""


def read_source(source: CsvSource, encoding: str = 'utf-8') -> bytes:
    """the CSV's bytes from a path, an open file, or bytes

    A ``str`` is always a path, never content: a mistyped filename must fail, not
    upload as a one-row CSV.
    """
    if isinstance(source, bytes):
        return source
    if isinstance(source, (str, os.PathLike)):
        try:
            with open(source, 'rb') as f:
                return f.read()
        except OSError as e:
            # the same error on every platform: Windows refuses a "path" holding newlines
            # (CSV text passed as a str) with a generic OSError rather than "not found"
            shown = str(source) if len(str(source)) <= 60 else str(source)[:57] + '...'
            raise FileNotFoundError(
                f"no CSV file at {shown!r}: a str is a path. To pass the CSV's content, "
                f"pass it as bytes (text.encode()).") from e
    data = source.read()
    return data.encode(encoding) if isinstance(data, str) else data


def parse_csv(source: CsvSource, delimiter: str = ',', encoding: str = 'utf-8'
              ) -> Tuple[List[str], List[List[Optional[str]]]]:
    """the header and the rows; an empty cell is ``None`` (missing)

    Header required; quotes, doubled quotes and quoted newlines as the engine's
    SQL ``COPY`` CSV parser reads them.
    """
    text = read_source(source, encoding).decode(encoding)
    if text.startswith('﻿'):
        text = text[1:]
    records = list(csv.reader(io.StringIO(text, newline=''), delimiter=delimiter, quotechar='"',
                              doublequote=True, strict=True))
    if not records:
        raise CsvFormatError("CSV import: the body is empty (a header row is required)")
    header = []
    for i, name in enumerate(records[0]):
        if not name.strip():
            raise CsvFormatError(f"CSV import: header column {i + 1} is empty")
        header.append(name.strip())
    duplicates = sorted({h for h in header if header.count(h) > 1})
    if duplicates:
        raise CsvFormatError(f"CSV import: duplicate header column(s) {', '.join(duplicates)}")
    rows = []
    for number, record in enumerate(records[1:], start=2):
        if not record:
            continue  # a blank line
        if len(record) != len(header):
            raise CsvFormatError(
                f"CSV import: row {number} has {len(record)} cells; the header has {len(header)}")
        rows.append([cell if cell != '' else None for cell in record])
    return header, rows


def _leading_zero(s: str) -> bool:
    return len(s) > 1 and s.startswith('0') and not s.startswith('0.')


def _column_kind(cells: Sequence[str]) -> str:
    if not cells or any(_leading_zero(c) for c in cells):
        return 'String'
    if all(_INT_RE.match(c) and _LONG_MIN <= int(c) <= _LONG_MAX for c in cells):
        return 'Int' if all(_INT_MIN <= int(c) <= _INT_MAX for c in cells) else 'Long'
    if all(_DECIMAL_RE.match(c) for c in cells):
        return 'Decimal'
    if all(c.lower() in ('true', 'false') for c in cells):
        return 'Boolean'
    return 'String'


def looks_like_text(values: Sequence[str]) -> bool:
    """free text rather than a label: the engine's ``StringStats.looksLikeText``

    More than half the values multi-word, their average length over 15, and more
    distinct values (counted up to 64) than min(20, n // 2).
    """
    n = len(values)
    if n == 0:
        return False
    multi_word = sum(1 for v in values if ' ' in v.strip())
    distinct = set()
    for v in values:
        if len(distinct) >= _DISTINCT_CAP:
            break
        distinct.add(v)
    return (multi_word / n > 0.5
            and sum(len(v) for v in values) / n > 15
            and len(distinct) > min(20, n // 2))


def infer_columns(header: List[str], rows: List[List[Optional[str]]]) -> Dict[str, Dict]:
    """a ``create_collection`` column map, typed as the engine types a CSV import"""
    columns = {}
    for i, name in enumerate(header):
        cells = [r[i] for r in rows if r[i] is not None]
        kind = _column_kind(cells)
        if kind == 'String' and looks_like_text(cells):
            kind = 'Text'   # no analyzer: what {"type": "Text"} gets, as in the engine
        spec = {'type': kind}
        if len(cells) < len(rows) or not cells:
            spec['nullable'] = True
        columns[name] = spec
    return columns


def infer_csv(source: CsvSource, delimiter: str = ',', encoding: str = 'utf-8') -> Dict[str, Dict]:
    """the column types an upload of this CSV would create, without uploading it"""
    return infer_columns(*parse_csv(source, delimiter, encoding))


def _convert(value: str, column_type: str, row: int, column: str):
    t = column_type.lower()
    try:
        if t in ('int', 'long'):
            if not _INT_RE.match(value):
                raise ValueError
            return int(value)
        if t == 'decimal':
            if not _DECIMAL_RE.match(value):
                raise ValueError
            return float(value)
        if t == 'boolean':
            if value.lower() not in ('true', 'false'):
                raise ValueError
            return value.lower() == 'true'
    except ValueError:
        raise ValueError(f"row {row}, column '{column}': {value!r} is not a{'n' if t == 'int' else ''} "
                         f"{column_type}") from None
    return value


def typed_rows(header: List[str], rows: List[List[Optional[str]]], columns: Dict[str, Dict]) -> List[Dict]:
    """the rows as JSON-ready dicts, each cell converted to its column's type

    A missing cell is left out of its row. A cell that does not fit its column's
    type raises ``ValueError`` naming the row (as numbered in the file, header = 1)
    and the column, before anything is sent. A column absent from ``columns`` is
    sent as a string, for the engine to accept or refuse.
    """
    types = {name: (spec.get('type') if isinstance(spec, dict) else str(spec)) for name, spec in columns.items()}
    out = []
    for number, r in enumerate(rows, start=2):
        out.append({name: _convert(v, types.get(name) or 'String', number, name)
                    for name, v in zip(header, r) if v is not None})
    return out
