"""Start real containers and execute the real HTTP/broker acceptance tests."""
import subprocess
import sys
from pathlib import Path

if __name__ == '__main__':
    root = Path(__file__).resolve().parent
    for action in ('up', 'test'):
        result = subprocess.call([sys.executable, 'scripts/deploy.py', action], cwd=root)
        if result:
            raise SystemExit(result)
