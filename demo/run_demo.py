"""Run the offline chaos demo. Usage: python demo/run_demo.py [--seed N] [--events N]

Equivalent to: python -m webhook_kit demo
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from webhook_kit.__main__ import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main(["demo", *sys.argv[1:]]))
