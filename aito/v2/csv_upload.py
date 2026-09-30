"""``Client.upload_csv``: the engine imports a CSV when it can, else it is typed here

Design: ``docs/adr/0003-sdk-csv-upload.md``. The typing rules are in
``aito._csv_types`` and mirror the engine's (aito-core #1535).
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from aito import _csv_types
from .errors import AitoV2Error

LOG = logging.getLogger('aito.v2')

_VIA = ('auto', 'server', 'client')
#: What an engine without CSV import answers when it parses a CSV body as JSON, as
#: observed on v2.11.1: `json.malformed`, or (for a quoted header) `data.bad_request`
#: with exactly this message. Only those trigger the fallback. An engine WITH CSV import
#: (v2.11.3+) reports a malformed file as `import.csv_invalid`; that, and any other
#: `data.bad_request` (a schema mismatch, say), is raised, never retried here.
_OLD_ENGINE_BAD_REQUEST = 'Expected JSON array for import'


@dataclass
class CsvUploadResult:
    """what ``upload_csv`` did"""

    #: the collection
    table: str
    #: rows inserted
    rows: int
    #: column name -> type (``'Text'``, ``'Decimal'``, ...), as created or as declared
    inferred: Dict[str, str]
    #: ``'server'``: the engine imported the CSV; ``'client'``: typed here, same rules
    via: str
    #: whether the collection was created by this call
    created: bool
    warnings: List[str] = field(default_factory=list)


def _engine_message(e: AitoV2Error) -> str:
    """the engine's own message, without the client's "returned 400 [code]" prefix"""
    try:
        return json.loads(e.body)['data']['message'].strip()
    except (TypeError, ValueError, KeyError):
        return ''


def _is_old_engine_refusal(e: AitoV2Error) -> bool:
    """the engine read the CSV as JSON, so it has no CSV import (not a CSV problem)"""
    if e.status_code != 400:
        return False
    if e.code == 'json.malformed':
        return True
    return e.code == 'data.bad_request' and _engine_message(e) == _OLD_ENGINE_BAD_REQUEST


def _type_names(columns: Dict) -> Dict[str, str]:
    return {name: (spec.get('type') if isinstance(spec, dict) else str(spec)) for name, spec in columns.items()}


def _text_warnings(columns: Dict, overridden: Dict) -> List[str]:
    return [f"{name}: inferred as Text (free text); pass schema={{'{name}': {{'type': 'Text', 'analyzer': ...}}}} "
            f"to choose an analyzer" for name, spec in columns.items()
            if spec.get('type') == 'Text' and name not in overridden]


def upload_csv(client, name: str, source, *, schema: Optional[Dict] = None, via: str = 'auto',
               delimiter: str = ',', encoding: str = 'utf-8', batch_size: int = 1000) -> CsvUploadResult:
    if via not in _VIA:
        raise ValueError(f"invalid via '{via}', expected one of {'|'.join(_VIA)}")
    if via == 'server' and (schema or delimiter != ','):
        raise ValueError("via='server' imports a comma-separated CSV with the engine's own inference; "
                         "schema= and other delimiters need via='client' (or 'auto')")
    body = _csv_types.read_source(source, encoding)
    header, rows = _csv_types.parse_csv(body, delimiter, encoding)

    try:
        existing = client.get_schema(name)
    except AitoV2Error as e:
        if not e.is_not_found:
            raise
        existing = None

    if existing is not None:
        # Append: converted here, so a cell that does not fit its declared type is
        # named before anything is sent (the engine would say only VALUE_STRING)
        declared = existing.get('columns', {})
        entries = _csv_types.typed_rows(header, rows, declared)
        count = client.upload_entries(name, entries, batch_size=batch_size)
        LOG.debug("upload_csv %s: appended %d rows via='client'", name, count)
        return CsvUploadResult(name, count, _type_names(declared), 'client', created=False)

    # schema= and other delimiters are applied here, so they take the client path
    if via != 'client' and delimiter == ',' and not schema:
        try:
            res = client.request('POST', f'/data/{name}/import', data=body, content_type='text/csv')
            count = int(res.get('count', len(rows)))
            inferred = _type_names(res.get('inferred') or {})
            warnings = [w.get('message', str(w)) if isinstance(w, dict) else str(w) for w in res.get('warnings', [])]
            LOG.debug("upload_csv %s: %d rows via='server'", name, count)
            return CsvUploadResult(name, count, inferred, 'server', created=True, warnings=warnings)
        except AitoV2Error as e:
            if via == 'server' or not _is_old_engine_refusal(e):
                raise
            LOG.debug("upload_csv %s: this engine has no CSV import (%s), typing it here", name, e.code)

    columns = _csv_types.infer_columns(header, rows)
    columns.update(schema or {})
    client.create_collection(name, columns)
    entries = _csv_types.typed_rows(header, rows, columns)
    count = client.upload_entries(name, entries, batch_size=batch_size)
    LOG.debug("upload_csv %s: %d rows via='client'", name, count)
    return CsvUploadResult(name, count, _type_names(columns), 'client', created=True,
                           warnings=_text_warnings(columns, schema or {}))
