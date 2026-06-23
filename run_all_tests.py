#!/usr/bin/env python3
'''Run every project test and print one explicit status line per case.'''

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
TEST_DIR = ROOT_DIR / 'tests'


class PassingTextTestResult(unittest.TextTestResult):
    '''Use a presentation-friendly PASSING label for successful test cases.'''

    def getDescription(self, test: unittest.case.TestCase) -> str:
        return test.id()

    def addSuccess(self, test: unittest.case.TestCase) -> None:
        unittest.TestResult.addSuccess(self, test)
        if self.showAll:
            self.stream.writeln('PASSING')
        elif self.dots:
            self.stream.write('.')
            self.stream.flush()


def load_all_tests() -> unittest.TestSuite:
    '''Discover all test_*.py files below the project's tests directory.'''
    loader = unittest.TestLoader()
    return loader.discover(
        start_dir=str(TEST_DIR),
        pattern='test_*.py',
        top_level_dir=str(ROOT_DIR),
    )


def main() -> int:
    sys.path.insert(0, str(ROOT_DIR))
    suite = load_all_tests()
    total = suite.countTestCases()

    print(f'Running {total} test cases\n')
    runner = unittest.TextTestRunner(
        stream=sys.stdout,
        verbosity=2,
        resultclass=PassingTextTestResult,
    )
    result = runner.run(suite)

    passed = result.testsRun - len(result.failures) - len(result.errors)
    if result.wasSuccessful():
        print(f'\nALL TESTS PASSING: {passed}/{total}')
        return 0

    print(
        f'\nTESTS FAILED: {passed}/{total} passing, '
        f'{len(result.failures)} failures, {len(result.errors)} errors'
    )
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
