#!/usr/bin/env python
"""
Run PyStore tests and generate coverage report
"""

import subprocess
import sys


def run_tests():
    """Run the test suite with coverage"""
    
    print("Running PyStore test suite...")
    print("=" * 60)
    
    # Run tests with pytest and coverage
    cmd = [
        sys.executable, "-m", "pytest",
        "-v",  # Verbose output
        "--cov=pystore",  # Coverage for pystore module
        "--cov-report=term-missing",  # Show missing lines
        "--cov-report=html",  # Generate HTML report
        "tests/"
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n" + "=" * 60)
        print("✅ All tests passed!")
        print("Coverage report generated in htmlcov/index.html")
        return 0
    except subprocess.CalledProcessError as e:
        print("\n" + "=" * 60)
        print("❌ Some tests failed!")
        return e.returncode
    except FileNotFoundError:
        print("❌ pytest not found. Install with: pip install pytest pytest-cov")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())