"""
Allow running the diagnostic as a module.

Usage:
    python -m eoql.diagnostic /path/to/repo
"""

from .cli import main
import sys

if __name__ == "__main__":
    sys.exit(main())
