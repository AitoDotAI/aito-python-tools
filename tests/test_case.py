import filecmp
import logging
from datetime import datetime, timezone
import unittest
from pathlib import Path
from typing import Optional, Union
import json
import ndjson

from aito.utils.generic_utils import root_path


class TestCaseTimer(unittest.TestCase):
    def setUp(self) -> None:
        self._started_at = datetime.now(timezone.utc)

    def tearDown(self) -> None:
        self._finished_at = datetime.now(timezone.utc)
        self._elapsed = self._finished_at - self._started_at


class TestCaseCompare(TestCaseTimer):
    """
    The TestCase create a input and a output folder and supports file comparison
    """

    @classmethod
    def setUpClass(
            cls,
            io_folder_path: Optional[Union[Path, str]] = 'tests/io',
            in_folder_name: Optional[str] = 'in',
            out_folder_name: Optional[str] = 'out',
            test_path: Optional[Union[Path, str]] = None
    ):
        """
        :param io_folder_path: Path to test io folder from project root
        :param in_folder_name: Input folder name
        :param out_folder_name: Output folder name
        :param test_path: Path to test class io folder if specified, else use class name
        :return:
        """
        io_path = root_path().joinpath(io_folder_path)
        if not test_path:
            test_path = cls.__name__
        cls.input_folder = (io_path / in_folder_name).joinpath(test_path)
        cls.output_folder = (io_path / out_folder_name).joinpath(test_path)
        cls.output_folder.mkdir(parents=True, exist_ok=True)

    def setUp(self):
        super().setUp()
        self.method_name = self.id().split('.')[-1]
        self.out_file_path = self.output_folder / (self.method_name + '_out.txt')
        self.exp_file_path = self.output_folder / (self.method_name + '_exp.txt')
        self.logger = logging.getLogger(self.method_name)

    def compare_file(self, out_file_path: Path, exp_file_path: Path, msg=None):
        self.logger.verbose(f'Comparing {out_file_path} with {exp_file_path}')
        if not msg:
            self.assertTrue(filecmp.cmp(str(out_file_path), str(exp_file_path), shallow=False),
                            f"out file {str(out_file_path)} does not match exp file {str(exp_file_path)}")
        else:
            self.assertTrue(filecmp.cmp(str(out_file_path), str(exp_file_path), shallow=False), msg)

    def compare_default_out_exp_file(self):
        self.compare_file(self.out_file_path, self.exp_file_path)

    def compare_json_files(
            self,
            out_file_path: Path,
            exp_file_path: Path,
            compare_order: bool = False,
            is_ndjson_file: bool = False
    ):
        self.logger.verbose(f'Comparing {out_file_path} with {exp_file_path}')
        load_method = ndjson.load if is_ndjson_file else json.load
        assert_method = self.assertEqual if compare_order else self.assertCountEqual
        with out_file_path.open() as out_f, exp_file_path.open() as exp_f:
            assert_method(load_method(out_f), load_method(exp_f))
