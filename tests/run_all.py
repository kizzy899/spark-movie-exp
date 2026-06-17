#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
缁熶竴娴嬭瘯鍏ュ彛銆? 杩愯 tests/ 鐩綍涓嬫墍鏈?test_*.py 娴嬭瘯銆? 
鐢ㄦ硶锛?     python run_all.py
    pytest tests/ -v
"""

import sys
import unittest
from pathlib import Path


def main():
    test_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(test_dir.parent))

    loader = unittest.TestLoader()
    suite = loader.discover(str(test_dir), pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
