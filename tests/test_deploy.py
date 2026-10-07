from pathlib import Path
import os
import yaml
from scripts.deploy import prepare_env, CREDENTIALS


def test_generated_credentials_are_stable_and_not_committed(tmp_path):
    example = Path(__file__).resolve().parents[1] / '.env.example'
    (tmp_path / '.env.example').write_text(example.read_text())
    path = prepare_env(tmp_path)
    first = path.read_text()
    assert 'change-me' not in first
    assert all(key + '=' in first for key in CREDENTIALS)
    assert prepare_env(tmp_path).read_text() == first
    assert os.stat(path).st_mode & 0o777 == 0o600


def test_compose_has_all_services_persistence_and_only_gateway_port():
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load((root / 'docker-compose.yml').read_text())
    services = compose['services']
    assert {'user', 'product', 'order', 'seckill', 'ai', 'gateway', 'db-init', 'mysql', 'redis', 'rabbitmq'} <= services.keys()
    assert [name for name, service in services.items() if service.get('ports')] == ['gateway']
    for service in ['mysql', 'redis', 'rabbitmq']:
        assert services[service]['volumes']
    for service in ['user', 'product', 'order', 'seckill', 'ai', 'gateway']:
        assert services[service]['healthcheck']
    assert '--appendfsync always' in services['redis']['command'][-1]
    assert '--maxmemory-policy noeviction' in services['redis']['command'][-1]
