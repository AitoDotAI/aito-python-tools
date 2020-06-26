import filecmp
import json
import logging
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union
import os

import ndjson

from .parser import ROOT_PATH


class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        class_path = Path(sys.modules[cls.__module__].__file__)
        cls.test_dir_path = ROOT_PATH / class_path.relative_to(ROOT_PATH).parts[0]
        cls.relative_to_test_dir = class_path.relative_to(cls.test_dir_path).parent

    def setUp(self) -> None:
        self._started_at = datetime.now(timezone.utc)

    def tearDown(self) -> None:
        self._finished_at = datetime.now(timezone.utc)
        self._elapsed = self._finished_at - self._started_at


class CompareTestCase(BaseTestCase):
    """
    The TestCase create a input and a output folder and supports file comparison
    """

    @classmethod
    def setUpClass(
            cls, in_folder_path: Optional[Union[Path, str]] = None,out_folder_path: Optional[Union[Path, str]] = None
    ):
        """
        :param in_folder_path: Input folder path if specified, else test_dir/io/in/path_to_case/case_name
        :param out_folder_path: Output folder Path if specified, else test_dir/io/out/path_to_case/case_name
        :return:
        """
        super().setUpClass()
        cls.input_folder = Path(in_folder_path) if in_folder_path \
            else cls.test_dir_path.joinpath(f'io/in/{cls.relative_to_test_dir}/{cls.__name__}')
        cls.output_folder = Path(out_folder_path) if out_folder_path \
            else cls.test_dir_path.joinpath(f'io/out/{cls.relative_to_test_dir}/{cls.__name__}')
        cls.output_folder.mkdir(parents=True, exist_ok=True)

    def setUp(self):
        super().setUp()
        self.method_name = self.id().split('.')[-1]
        self.out_file_path = self.output_folder / (self.method_name + '_out.txt')
        self.exp_file_path = self.output_folder / (self.method_name + '_exp.txt')
        self.logger = logging.getLogger(self.method_name)

    def compare_file(self, out_file_path: Path, exp_file_path: Path, msg=None):
        self.logger.debug(f'Comparing {out_file_path} with {exp_file_path}')
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
        self.logger.debug(f'Comparing {out_file_path} with {exp_file_path}')
        load_method = ndjson.load if is_ndjson_file else json.load
        assert_method = self.assertEqual if compare_order else self.assertCountEqual
        with out_file_path.open() as out_f, exp_file_path.open() as exp_f:
            assert_method(load_method(out_f), load_method(exp_f))

    def stub_stdin(self, new_input):
        saved_stdin = sys.stdin

        def cleanup():
            sys.stdin = saved_stdin

        self.addCleanup(cleanup)
        sys.stdin = new_input
        self.logger.debug(f'new stdin {sys.stdin}')

    def stub_stdout(self, new_output):
        saved_stdout = sys.stdout

        def cleanup():
            sys.stdout = saved_stdout

        self.addCleanup(cleanup)
        sys.stdout = new_output
        self.logger.debug(f'new stdout {sys.stdout}')

    def stub_environment_variable(self, var_name: str, new_var_value):
        old_var_value = os.getenv(var_name)

        def cleanup():
            if old_var_value is not None:
                os.environ[var_name] = old_var_value

        self.addCleanup(cleanup)
        if new_var_value is not None:
            os.environ[var_name] = new_var_value
            self.logger.debug(f'stub env var `{var_name}`: {old_var_value} -> {new_var_value}')
        else:
            if var_name in os.environ:
                del os.environ[var_name]
            self.logger.debug(f'delete env var `{var_name}`')
