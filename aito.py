import sys

from aito.cli.main_parser import MainParser
from config import set_up_logger

if __name__ == '__main__':
    set_up_logger()
    main_parser = MainParser()
    main_parser.parse_and_execute(sys.argv[1:])

