"""Cross-platform deployment using only Python's standard library and Docker Compose."""
import argparse
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
CREDENTIALS = ('SECRET_KEY', 'MYSQL_PASSWORD', 'MYSQL_ROOT_PASSWORD', 'REDIS_PASSWORD', 'RABBITMQ_PASS', 'ADMIN_PASSWORD')


def prepare_env(root=ROOT):
    path = root / '.env'
    text = path.read_text(encoding='utf-8') if path.exists() else (root / '.env.example').read_text(encoding='utf-8')
    values = {}
    for line in text.splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip()
    updated = False
    for key in CREDENTIALS:
        value = values.get(key, '').strip('"\'')
        if not value or value.startswith('change-me'):
            generated = secrets.token_hex(32) if key == 'SECRET_KEY' else secrets.token_urlsafe(24)
            pattern = re.compile(r'^' + re.escape(key) + r'=.*$', re.MULTILINE)
            text = pattern.sub(key + '=' + generated, text) if pattern.search(text) else text.rstrip() + '\n' + key + '=' + generated + '\n'
            updated = True
    if updated or not path.exists():
        # Exclusive temporary file + atomic replace avoids partial credential files.
        temp = path.with_name('.env.tmp-' + secrets.token_hex(4))
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as file:
            file.write(text)
        os.replace(temp, path)
    return path


def run(args):
    subprocess.run(args, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description='Start the complete WebBackend stack')
    parser.add_argument('action', nargs='?', choices=['up', 'down', 'test', 'status', 'logs', 'check'], default='up')
    options = parser.parse_args()
    if shutil.which('docker') is None:
        parser.exit(1, 'Docker is missing. Install Docker Desktop (Windows/macOS) or Docker Engine + Compose v2 (Linux).\n')
    version = subprocess.run(['docker', 'compose', 'version', '--short'], capture_output=True, text=True)
    parsed = re.search(r'(\d+)\.(\d+)\.(\d+)', version.stdout)
    if version.returncode or not parsed or tuple(map(int, parsed.groups())) < (2, 20, 0):
        parser.exit(1, 'Docker Compose >= 2.20 is required.\n')
    path = prepare_env()
    base = ['docker', 'compose', '--env-file', str(path)]
    run(base + ['config', '--quiet'])
    if options.action == 'check':
        print('Configuration valid. No containers started.')
    elif options.action == 'up':
        run(base + ['up', '--build', '--detach', '--wait', '--wait-timeout', '240'])
        print('Services ready. Open http://localhost:8000/docs (or your configured GATEWAY_PORT).')
        print('Visualization lab: http://localhost:8000/lab (principle simulation + read-only admin observations).')
        print('Administrator credentials: ADMIN_USERNAME / ADMIN_PASSWORD in .env. Existing accounts are preserved.')
    elif options.action == 'test':
        run(base + ['--profile', 'test', 'run', '--build', '--rm', 'tests'])
    elif options.action == 'down':
        run(base + ['down'])  # deliberately preserve all named data volumes
    elif options.action == 'status':
        run(base + ['ps'])
    else:
        run(base + ['logs', '--tail', '100', '--follow'])


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print('Command failed. Inspect: docker compose ps; docker compose logs --tail 100', file=sys.stderr)
        sys.exit(exc.returncode or 1)
