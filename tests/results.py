import unittest
import subprocess
from pathlib import Path
import os


class TestResultLogMetrics(unittest.TextTestResult):
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.metrics_log_path = Path(os.environ['METRICS_LOG_PATH']) if os.environ.get('METRICS_LOG_PATH') else None

    def addSuccess(self, test):
        super().addSuccess(test)
        if self.metrics_log_path:
            with self.metrics_log_path.open(mode='a+') as f:
                f.write(f"{getattr(test, '_started_at', '')}, {test.id()}, success, {getattr(test, '_elapsed', '')},\n")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        if self.metrics_log_path:
            with self.metrics_log_path.open(mode='a+') as f:
                f.write(f"{getattr(test, '_started_at', '')}, {test.id()}, fail, {getattr(test, '_elapsed', '')}, "
                        f"{err}\n")

    def addError(self, test, err):
        super().addError(test, err)
        if self.metrics_log_path:
            with self.metrics_log_path.open(mode='a+') as f:
                f.write(f"{getattr(test, '_started_at', '')}, {test.id()}, error, {getattr(test, '_elapsed', '')}, "
                        f"{err}\n")


class TestResultCompareFileMeld(TestResultLogMetrics):
    def addFailure(self, test, err):
        super().addFailure(test, err)
        out_file_path = getattr(test, 'out_file_path')
        exp_file_path = getattr(test, 'exp_file_path')
        if out_file_path and exp_file_path:
            if Path(out_file_path).exists() and Path(exp_file_path).exists():
                cont = True
                while cont:
                    res = input("[d]iff, [c]ontinue, or [f]reeze? ")
                    if res == "f":
                        os.rename(out_file_path, exp_file_path)
                        cont = False
                    elif res == "c":
                        cont = False
                    elif res == "d":
                        subprocess.run(['meld', str(out_file_path), str(exp_file_path)])
                    else:
                        print('must input d, c, or f')