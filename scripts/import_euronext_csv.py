"""Import a manually downloaded Euronext equities CSV (fallback for Flow A step 1)."""

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.parse_args()
    raise NotImplementedError


if __name__ == "__main__":
    main()
