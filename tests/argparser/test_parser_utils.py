from aito.cli.parser_utils import *
from tests.cases import CompareTestCase
from aito.cli.sub_commands.infer_table_schema import InferFromFormatSubCommand
from aito.cli.sub_commands.convert import ConvertFromFormatSubCommand


class TestParserUtils(CompareTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.input_folder = cls.input_folder.parent.parent / 'sample_invoice'

    def test_parse_env_variable(self):
        self.assertIsNone(parse_env_variable('RADIO_GA_GA'))
        with self.assertRaises(ParseError):
            parse_env_variable('RADIO_GA_GA', True)

    def test_parsed_args_to_df_handler_read_args(self):
        self.assertEqual(
            {'read_input': sys.stdin, 'in_format': 'json', 'read_options': {'encoding': 'utf-8'}},
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'json', 'input': '-', 'encoding': 'utf-8'}
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.json',
                'in_format': 'ndjson',
                'read_options': {'encoding': 'utf-8'}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'ndjson', 'input': str(self.input_folder / 'invoice.json'), 'encoding': 'utf-8'}
            )
        )

        self.assertEqual(
            {
                'read_input': sys.stdin,
                'in_format': 'csv',
                'read_options': {'encoding': 'utf-8', 'delimiter': ',', 'decimal': '.'}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'csv', 'input': '-', 'encoding': 'utf-8', 'delimiter': ',', 'decimal': '.'}
            )
        )

        self.assertEqual(
            {
                'read_input': self.input_folder / 'invoice.xlsx',
                'in_format': 'excel',
                'read_options': {'encoding': 'utf-8', 'sheet_name': 'sheet_name'}
            },
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {
                    'input-format': 'excel',
                    'input': str(self.input_folder / 'invoice.xlsx'),
                    'encoding': 'utf-8',
                    'one_sheet': 'sheet_name'
                }
            )
        )

        with self.assertRaises(ParseError, msg = 'input must be a file path for excel files'):
            InferFromFormatSubCommand.parsed_args_to_data_frame_handler_read_args(
                {'input-format': 'excel', 'input': '-', 'encoding': 'utf-8', 'one_sheet': None}
            )
