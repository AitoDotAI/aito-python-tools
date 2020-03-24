import gzip
import json
import os
import shutil
from pathlib import Path
from typing import Dict, List

import ndjson


def gzip_file(input_path: Path, output_path: Path = None, keep=True):
    if not output_path:
        output_path = input_path.parent / f"{input_path.name}.gz"
    with input_path.open('rb') as f_in, gzip.open(output_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    if not keep:
        os.unlink(input_path)


def read_json_gz_file(input_path: Path, decoder='utf-8'):
    with gzip.open(input_path, 'rb') as in_f:
        json_bytes = in_f.read()
    return json.loads(json_bytes.decode(decoder))


def read_ndjson_gz_file(input_path: Path, decoder='utf-8'):
    records = []
    with gzip.open(input_path, 'rb') as in_f:
        line = in_f.readline()
        while line:
            records.append(json.loads(line.decode(decoder)))
            line = in_f.readline()
    return records


def write_to_ndjson_gz_file(data: List[Dict], output_file: Path):
    if not output_file.name.endswith(".ndjson.gz"):
        raise ValueError("Output file must end with .ndjson.gz")
    ndjson_file = output_file.parent / output_file.stem
    with ndjson_file.open('w') as f:
        ndjson.dump(data, f)
    gzip_file(ndjson_file, output_file, keep=False)
