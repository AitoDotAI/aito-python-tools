from aito.cli.main_parser import MainParser
import sys

if __name__ == '__main__':
    main_parser = MainParser()
    main_parser.parse_and_execute(sys.argv[1:])

