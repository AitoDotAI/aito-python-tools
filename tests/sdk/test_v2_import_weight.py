"""The client and the CLI must stay cheap to import

`from aito.client.v2 import AitoClientV2` used to load pandas, numpy, aiohttp
and langdetect — the whole v1 stack, because importing a submodule runs the
parent package's ``__init__``, and that chain reached ``aito/schema.py``. The v2
client is a plain HTTP client whose only third-party dependency is ``requests``.

The cost was not only weight. On a machine where numpy's compiled extensions
cannot load, ``import aito.client.v2`` raised — so a service that had never
touched a dataframe stopped starting the moment it depended on this SDK. That
is how it was found: the first real consumer (aito-erp-demo) could not import
the client it had just adopted.

The CLI has the same requirement for the same reason: building its parser
imports every subcommand module, so `aito -V` used to load a dataframe library
before printing a version string.

These run in a **subprocess**. An in-process check is worthless: by the time
this module executes, another test in the same run has almost certainly
imported pandas already, and the assertion would pass no matter what.
"""

import subprocess
import sys

from tests.cases import BaseTestCase

#: Imported for their side effects on install size and start-up, and none of
#: them are needed to send an HTTP request. numpy is the one that also *fails*
#: where its compiled extensions are unavailable.
HEAVY = ('pandas', 'numpy', 'aiohttp', 'langdetect')


def _modules_loaded_by(statement: str) -> set:
    """return the names in :data:`HEAVY` that `statement` pulls in"""
    probe = (
        f'import sys; {statement}; '
        f'print(",".join(m for m in {HEAVY!r} if m in sys.modules))'
    )
    result = subprocess.run([sys.executable, '-c', probe],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise AssertionError(
            f'`{statement}` failed to import:\n{result.stderr[-2000:]}')
    return {name for name in result.stdout.strip().split(',') if name}


class TestV2ImportWeight(BaseTestCase):
    def test_importing_the_v2_client_stays_light(self):
        loaded = _modules_loaded_by('import aito.client.v2')
        self.assertEqual(loaded, set(), f'importing the v2 client pulled in {sorted(loaded)}')

    def test_importing_the_public_name_stays_light(self):
        # The line a stranger actually writes.
        loaded = _modules_loaded_by('from aito.client.v2 import AitoClientV2')
        self.assertEqual(loaded, set(), f'importing AitoClientV2 pulled in {sorted(loaded)}')

    def test_the_v1_client_is_also_light_to_import(self):
        # Not the headline case, but it falls out of the same change and is
        # worth holding: aiohttp is only needed for the async path.
        loaded = _modules_loaded_by('from aito.client import AitoClient')
        self.assertEqual(loaded, set(), f'importing AitoClient pulled in {sorted(loaded)}')

    def test_building_the_cli_parser_stays_light(self):
        """`aito -V` must not load a dataframe library.

        Building the parser imports every subcommand module, and one of them
        reaches `aito/utils/data_frame_handler.py`. That made the whole CLI —
        including commands that touch no data, like `-V`, `list` and the
        planned `server start` — pay for pandas, and fail outright wherever
        numpy could not load. Only `convert` and the file-reading commands
        genuinely need it, and they now pull it when they run.
        """
        loaded = _modules_loaded_by(
            'from aito.cli.main_parser import MainParser; MainParser()')
        self.assertEqual(loaded, set(), f'building the CLI parser pulled in {sorted(loaded)}')

    def test_the_cli_still_converts_a_file(self):
        """The deferred pandas must still arrive for the commands that need it."""
        probe = (
            'import io, sys;'
            'from aito.utils.data_frame_handler import DataFrameHandler;'
            'buf = io.StringIO("id,name\\n1,Neil\\n2,Buzz\\n");'
            'df = DataFrameHandler().read_file_to_df(buf, "csv");'
            'print(len(df), sorted(df.columns))'
        )
        result = subprocess.run([sys.executable, '-c', probe],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        self.assertEqual(result.stdout.strip(), "2 ['id', 'name']")

    def test_the_deferred_dependencies_still_work_when_actually_used(self):
        """Deferring an import must not mean losing the feature.

        Schema inference genuinely needs pandas, and the async path genuinely
        needs aiohttp — both must still load on demand.
        """
        probe = (
            'import sys, pandas as pd;'
            'from aito.schema import AitoTableSchema;'
            'df = pd.DataFrame(data={"id": [1, 2], "name": ["Neil", "Buzz"]});'
            'schema = AitoTableSchema.infer_from_pandas_data_frame(df);'
            'print(sorted(schema.columns))'
        )
        result = subprocess.run([sys.executable, '-c', probe],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        self.assertEqual(result.stdout.strip(), "['id', 'name']")

    def test_language_inference_still_works(self):
        """`langdetect` is deferred inside `AitoAnalyzerSchema._infer_language`."""
        probe = (
            'from aito.schema import AitoAnalyzerSchema, AitoLanguageAnalyzerSchema;'
            'samples = ["the quick brown fox jumps over the lazy dog",'
            '           "she sells sea shells by the sea shore today",'
            '           "all work and no play makes jack a dull boy"] * 4;'
            'analyzer = AitoAnalyzerSchema.infer_from_samples(samples);'
            'print(isinstance(analyzer, AitoLanguageAnalyzerSchema) and analyzer.language)'
        )
        result = subprocess.run([sys.executable, '-c', probe],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        # langdetect actually ran and named a language, so the deferred import
        # resolved rather than silently degrading to the whitespace fallback.
        self.assertEqual(result.stdout.strip(), 'english')
