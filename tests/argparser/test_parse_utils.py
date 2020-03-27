from aito.cli.parse_utils import *
from tests.cases import TestCaseCompare
from aito.cli.sub_commands.infer_table_schema import InferFromFormatSubCommand
from aito.cli.sub_commands.convert import ConvertFromFormatSubCommand


class TestParseUtils(TestCaseCompare):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(test_path='cli/parsing')
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'

    def test_parse_env_variable(self):
        self.assertIsNone(parse_env_variable('RADIO_GA_GA'))
        with self.assertRaises(ParseError):
            parse_env_variable('RADIO_GA_GA', True)

    def test_parse_path_value(self):
        with self.assertRaises(ParseError):
            parse_path_value('../i_want_to_break_free.py', True)

    def test_parse_io_arg_value(self):
        self.assertEqual(sys.stdin, parse_input_arg_value('-'))
        self.assertEqual(Path('test_parse_utils.py'), parse_output_arg_value('test_parse_utils.py'))

    def test_parsed_args_to_df_handler_read_args(self):
        self.assertEqual(
            {'read_input': sys.stdin, 'in_format': 'json', 'read_options': {'encoding': None}},
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'json', 'input': '-', 'encoding': None}
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.json',
                'in_format': 'ndjson',
                'read_options': {'encoding': None}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'ndjson', 'input': str(self.input_folder / 'invoice.json'), 'encoding': None}
            )
        )

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'in_format': 'csv',
                'read_options': {'encoding': None, 'delimiter': ',', 'decimal': '.'}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'csv', 'input': '-', 'encoding': None, 'delimiter': ',', 'decimal': '.'}
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.xlsx',
                'in_format': 'excel',
                'read_options': {'encoding': None, 'sheet_name': 'sheet_name'}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {
                    'input-format': 'excel',
                    'input': str(self.input_folder / 'invoice.xlsx'),
                    'encoding': None,
                    'one_sheet': 'sheet_name'
                }
            )
        )

        with self.assertRaises(ParseError, msg = 'input must be a file path for excel files'):
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'excel', 'input': '-', 'encoding': None, 'one_sheet': None}
            )

    def test_parse_args_to_df_handler_convert_args(self):
        with (self.input_folder / 'invoice_aito_schema.json').open() as f:
            schema = json.load(f)

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'write_output': sys.stdout,
                'in_format': 'json',
                'out_format': 'ndjson',
                'read_options': {'encoding': None},
                'convert_options': {},
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                {'input-format': 'json', 'input': '-', 'encoding': None, 'json': False, 'use_table_schema': False}
            )
        )

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'write_output': sys.stdout,
                'in_format': 'ndjson',
                'out_format': 'json',
                'read_options': {'encoding': None},
                'convert_options': {},
                'use_table_schema': schema
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                {
                    'input-format': 'ndjson',
                    'input': '-', 'encoding': None,
                    'json': True,
                    'use_table_schema': str(self.input_folder / 'invoice_aito_schema.json')
                }
            )
        )

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'write_output': sys.stdout,
                'in_format': 'csv',
                'out_format': 'ndjson',
                'read_options': {'encoding': None, 'delimiter': ',', 'decimal': '.'},
                'convert_options': {},
                'use_table_schema': schema
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                {
                    'input-format': 'csv',
                    'input': '-',
                    'encoding': None,
                    'json': False,
                    'use_table_schema': str(self.input_folder / 'invoice_aito_schema.json'),
                    'delimiter': ',',
                    'decimal': '.'
                }
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.xlsx',
                'write_output': sys.stdout,
                'in_format': 'excel',
                'out_format': 'ndjson',
                'read_options': {'encoding': None, 'sheet_name': 'sheet_name'},
                'convert_options': {},
            },
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                {
                    'input-format': 'excel',
                    'input': str(self.input_folder / 'invoice.xlsx'),
                    'encoding': None,
                    'json': False,
                    'use_table_schema': None,
                    'one_sheet': 'sheet_name'
                }
            )
        )

        with self.assertRaises(ParseError, msg = 'input must be a file path for excel files'):
            ConvertFromFormatSubCommand.parsed_args_to_data_frame_handler_convert_args(
                {
                    'input-format': 'excel',
                    'input': '-',
                    'encoding': None,
                    'json': False,
                    'use_table_schema': None,
                    'sheet_name': None
                }
            )
