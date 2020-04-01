from tests.cases import CompareTestCase
from argparse import ArgumentParser
from aito.cli.parser import ParseError, PathArgType, InputArgType, OutputArgType
from pathlib import Path
import sys


class TestParser(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.parser = ArgumentParser()

    def test_path_type(self):
        self.parser.add_argument('path', type=PathArgType())
        self.assertEqual(self.parser.parse_args(['some_path']).path, Path('some_path'))

    def test_path_type_parent_exists(self):
        self.parser.add_argument('path', type=PathArgType(parent_must_exist=True))
        self.assertEqual(
            self.parser.parse_args([f'{self.input_folder.parent}']).path,
            self.input_folder.parent
        )
        with self.assertRaises(SystemExit):
            self.parser.parse_args([str(self.input_folder / 'a_file')])

    def test_path_type_must_exists(self):
        self.parser.add_argument('path', type=PathArgType(must_exist=True))
        self.assertEqual(
            self.parser.parse_args([str(self.input_folder.parent.parent.joinpath("sample_invoice/invoice.csv"))]).path,
            self.input_folder.parent.parent / 'sample_invoice' / 'invoice.csv'
        )
        with self.assertRaises(SystemExit):
            self.parser.parse_args([str(self.input_folder.parent.parent.joinpath('i want to break free'))])

    def test_input_type(self):
        self.parser.add_argument('input', type=InputArgType(), default='-', nargs='?')
        self.assertEqual(self.parser.parse_args([]).input, sys.stdin)
        self.assertEqual(
            self.parser.parse_args([str(self.input_folder.parent.parent.joinpath("sample_invoice/invoice.csv"))]).input,
            self.input_folder.parent.parent / 'sample_invoice' / 'invoice.csv'
        )
        with self.assertRaises(SystemExit):
            self.parser.parse_args([str(self.input_folder.parent.parent.joinpath('i want to break free'))])

    def test_output_type(self):
        self.parser.add_argument('output', type=OutputArgType(), default='-', nargs='?')
        self.assertEqual(self.parser.parse_args([]).output, sys.stdout)
        self.assertEqual(
            self.parser.parse_args([str(self.input_folder.parent)]).output,
            self.input_folder.parent
        )
        with self.assertRaises(SystemExit):
            self.parser.parse_args([str(self.input_folder / 'a_file')])
