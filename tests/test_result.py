import os
import unittest


class TestResultCompareFileMeld(unittest.TextTestResult):
    def addFailure(self, test, err):
        super().addFailure(test, err)
        if hasattr(test, 'out_file') and hasattr(test, 'exp_file'):
            method_id = test.id().split('.')[-1]
            if method_id in test.out_file and method_id in test.exp_file:
                cont = True
                while cont:
                    res = input("[d]iff, [c]ontinue, or [f]reeze? ")
                    if res == "f":
                        os.rename(test.out_file[method_id], test.exp_file[method_id])
                        cont = False
                    elif res == "c":
                        cont = False
                    elif res == "d":
                        os.system("meld " + str(test.exp_file[method_id]) + " " + str(test.out_file[method_id]))