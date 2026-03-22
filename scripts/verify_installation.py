#!/usr/bin/env python3
"""Verify LSIEE installation."""

import sys


def check_python():
    """Check Python version."""
    version = sys.version_info
    if version.major == 3 and version.minor >= 10:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print(f"✗ Python 3.10+ required, found {version.major}.{version.minor}")
        return False


def check_dependencies():
    """Check if all required packages are installed."""
    required = ["click", "rich", "sklearn", "pandas", "psutil", "watchdog", "statsmodels"]

    all_ok = True
    for package in required:
        try:
            __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} (MISSING)")
            all_ok = False

    return all_ok


def check_lsiee():
    """Check if LSIEE is importable."""
    try:
        import lsiee

        print(f"✓ lsiee {lsiee.__version__}")
        return True
    except ImportError as e:
        print(f"✗ lsiee (MISSING): {e}")
        return False


def main():
    """Run all checks."""
    print("LSIEE Installation Verification")
    print("=" * 50)
    print()

    checks = [check_python(), check_dependencies(), check_lsiee()]

    print()
    if all(checks):
        print("✅ All checks passed! LSIEE is ready to use.")
        print("\nTry: lsiee --help")
        return 0
    else:
        print("❌ Some checks failed. Please review errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
