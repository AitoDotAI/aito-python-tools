import gzip
import json
import os
import shutil
from os import PathLike
from pathlib import Path
from typing import Dict, List

import ndjson


def check_file_is_gzipped(file_path: PathLike):
    file_path = Path(file_path)
    if file_path.suffixes[-2:] != ['.ndjson', '.gz']:
        return False
    else:
        return True


def gzip_file(input_path: PathLike, output_path: PathLike = None, keep=True):
    input_path = Path(input_path)
    if input_path.name.endswith('.gz'):
        raise ValueError(f'{input_path} is already gzipped')
    output_path = Path(output_path) if output_path else input_path.parent / f"{input_path.name}.gz"
    with input_path.open('rb') as f_in, gzip.open(output_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    if not keep:
        os.unlink(input_path)


def read_json_gz_file(input_path: PathLike, decoder='utf-8'):
    input_path = Path(input_path)
    with gzip.open(input_path, 'rb') as in_f:
        json_bytes = in_f.read()
    return json.loads(json_bytes.decode(decoder))


def read_ndjson_gz_file(input_path: PathLike, decoder='utf-8'):
    input_path = Path(input_path)
    records = []
    with gzip.open(input_path, 'rb') as in_f:
        line = in_f.readline()
        while line:
            records.append(json.loads(line.decode(decoder)))
            line = in_f.readline()
    return records


def write_to_ndjson_gz_file(data: List[Dict], output_file: PathLike):
    output_file = Path(output_file)
    if not output_file.name.endswith(".ndjson.gz"):
        raise ValueError("Output file must end with .ndjson.gz")
    ndjson_file = output_file.parent / output_file.stem
    with ndjson_file.open('w') as f:
        ndjson.dump(data, f)
    gzip_file(ndjson_file, output_file, keep=False)
