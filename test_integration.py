"""Compatibility entry point. Unit/mocked tests are not a network performance benchmark."""
import subprocess
import sys
from pathlib import Path

if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    raise SystemExit(subprocess.call([sys.executable, '-m', 'pytest', '-q', 'tests'], cwd=root))
