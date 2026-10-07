import uuid
from unittest.mock import AsyncMock
import httpx
from common import observation
from common.events import OrderEvent
from common.seckill_store import reserve, read_request, activity_key, OUTBOX
from services.seckill_service import main as seckill
from services.gateway import main as gateway
from services.order_service import main as order
from tests.test_boundaries import activity, client, headers


async def test_lab_public_page_but_observations_are_admin_only(tokens):
    async with client(gateway.app) as api:
        response = await api.get('/lab')
        assert response.status_code == 200 and '真实观测' in response.text
        assert "script-src 'self'" in response.headers['content-security-policy']
        for asset in ['lab.js', 'lab.css', 'scenarios.js']:
            assert (await api.get('/lab/assets/' + asset)).status_code == 200
        assert (await api.get('/lab/api/health')).status_code == 401
        assert (await api.get('/lab/api/health', headers=headers(tokens['customer']))).status_code == 403
    async with client(seckill.app) as api:
        for path in ['/seckill/observe/overview', '/seckill/observe/requests/' + str(uuid.uuid4())]:
            assert (await api.get(path)).status_code == 401
            assert (await api.get(path, headers=headers(tokens['customer']))).status_code == 403


async def test_observation_is_read_only_and_counts_committed_orders(tokens, isolated_state, monkeypatch):
    monkeypatch.setattr(observation, 'queue_observations', AsyncMock(return_value=[]))
    _, aid = await activity(tokens, stock=3)
    rid = str(uuid.uuid4())
    await reserve(aid, 2, rid)
    event = OrderEvent.model_validate({k:v for k,v in (await read_request(rid)).items() if k != 'state'})
    await order.create_order(event)
    async with client(seckill.app) as api:
        for _ in range(2):
            response = await api.get('/seckill/observe/overview', headers=headers(tokens['admin']))
            assert response.status_code == 200
            assert response.headers['cache-control'] == 'no-store'
            data = response.json()['data']
            assert data['atomic_snapshot'] is False
            assert data['outbox']['length'] == 1
            assert data['outbox']['preview'][0]['request_id'] == rid
            row = data['activities'][0]
            assert row['redis']['remaining_stock'] == 2 and row['redis']['accepted_users'] == 1
            assert row['orders_created'] == 1
        assert (await api.get('/seckill/observe/overview?limit=51', headers=headers(tokens['admin']))).status_code == 422
        response = await api.get('/seckill/observe/requests/' + rid, headers=headers(tokens['admin']))
        assert response.json()['data']['status'] == 'created'
        assert response.json()['data']['broker_location'] == 'not_tracked'
    assert await isolated_state.xlen(OUTBOX) == 1  # observation did not consume/delete the event
    assert await isolated_state.hget(activity_key(aid), 'stock') == '2'


async def test_missing_redis_never_looks_like_zero_inventory(tokens, isolated_state, monkeypatch):
    monkeypatch.setattr(observation, 'queue_observations', AsyncMock(return_value=[]))
    _, aid = await activity(tokens)
    await isolated_state.delete(activity_key(aid))
    data = await observation.overview()
    record = data['activities'][0]['redis']
    assert record['available'] is False and record['remaining_stock'] is None


async def test_request_uses_sql_after_redis_record_is_lost(tokens, isolated_state):
    from common.seckill_store import request_key
    _, aid = await activity(tokens)
    rid = str(uuid.uuid4())
    await reserve(aid, 2, rid)
    event = OrderEvent.model_validate({k:v for k,v in (await read_request(rid)).items() if k != 'state'})
    await order.create_order(event)
    await isolated_state.delete(request_key(rid))
    data = await observation.inspect_request(rid)
    assert data['status'] == 'created' and data['order']['request_id'] == rid
    assert data['redis_record'] is None


async def test_wrong_key_types_do_not_claim_healthy_zero(tokens, isolated_state, monkeypatch):
    monkeypatch.setattr(observation, 'queue_observations', AsyncMock(return_value=[]))
    _, aid = await activity(tokens)
    await isolated_state.delete(activity_key(aid))
    await isolated_state.set(activity_key(aid), 'broken')
    await isolated_state.set(OUTBOX, 'broken')
    data = await observation.overview()
    assert data['activities'][0]['redis']['available'] is False
    assert data['outbox']['available'] is False
    assert data['outbox']['length'] is None


async def test_gateway_health_preserves_each_service_failure(tokens):
    def handle(request):
        return httpx.Response(503 if request.url.port == 8003 else 200, json={'status':'ready'})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as upstream:
        gateway.app.state.http = upstream
        async with client(gateway.app) as api:
            response = await api.get('/lab/api/health', headers=headers(tokens['admin']))
    services = {row['name']:row for row in response.json()['data']['services']}
    assert services['order']['ready'] is False and services['user']['ready'] is True


async def test_unavailable_queue_statistics_are_unknown(monkeypatch):
    original = httpx.AsyncClient
    transport = httpx.MockTransport(lambda req: httpx.Response(503, text='unavailable'))
    monkeypatch.setattr(observation.httpx, 'AsyncClient', lambda **kwargs: original(transport=transport, **kwargs))
    queues = await observation.queue_observations()
    assert len(queues) == 3
    assert all(row['available'] is False and 'total' not in row for row in queues)
