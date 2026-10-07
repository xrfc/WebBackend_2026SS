"""Real HTTP + MySQL + Redis + RabbitMQ + WebSocket. Run only on an isolated test stack."""
import asyncio
import os
import uuid
from urllib.parse import urlsplit
import pytest
import websockets

pytestmark = [pytest.mark.live, pytest.mark.skipif(os.getenv('RUN_LIVE_TESTS') != '1', reason='real stack not configured')]
BASE_URL = os.getenv('LIVE_BASE_URL', 'http://localhost:8000')


def auth(token):
    return {'Authorization': 'Bearer ' + token}




async def admin_token(api):
    response = await api.post('/login', json={'username': os.getenv('ADMIN_USERNAME', 'admin'), 'password': os.environ['ADMIN_PASSWORD']})
    assert response.status_code == 200, response.text
    return response.json()['data']['token']


async def customer(api):
    name = 'test_' + uuid.uuid4().hex[:16]
    password = 'test-password-123'
    response = await api.post('/register', json={'username': name, 'password': password})
    assert response.status_code == 200, response.text
    response = await api.post('/login', json={'username': name, 'password': password})
    assert response.status_code == 200, response.text
    return response.json()['data']


async def setup_activity(api, admin, stock=5):
    response = await api.post('/products', headers=auth(admin),
        json={'name': 'live_' + uuid.uuid4().hex[:8], 'price': '99.99', 'stock': stock + 1})
    assert response.status_code == 200, response.text
    pid = response.json()['data']['id']
    response = await api.post('/seckill/start', headers=auth(admin),
        json={'product_id': pid, 'stock': stock, 'seckill_price': '0.01'})
    assert response.status_code == 200, response.text
    return pid, response.json()['data']['activity_id']


async def await_order(api, token, request_id, timeout=60):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        response = await api.get('/seckill/requests/' + request_id, headers=auth(token))
        assert response.status_code == 200, response.text
        data = response.json()['data']
        if data['status'] == 'created':
            return data
        assert data['status'] == 'queued', data
        await asyncio.sleep(0.2)
    pytest.fail('Accepted request did not produce a durable order before the deadline')


async def test_real_end_to_end_stock_orders_and_websocket(api):
    assert (await api.get('/ready')).status_code == 200
    schema = (await api.get('/openapi.json')).json()
    assert {'/login', '/seckill/start', '/seckill/submit', '/orders', '/ai/consult'} <= schema['paths'].keys()
    admin = await admin_token(api)
    people = [await customer(api) for _ in range(8)]
    pid, aid = await setup_activity(api, admin)
    target = urlsplit(BASE_URL)
    url = f'{"wss" if target.scheme == "https" else "ws"}://{target.netloc}/ws/{people[0]["user"]["id"]}'
    async with websockets.connect(url, open_timeout=5) as socket:
        await socket.send(__import__('json').dumps({'token': people[0]['token']}))
        connected = __import__('json').loads(await asyncio.wait_for(socket.recv(), 10))
        assert connected['type'] == 'connected'
        first = await api.post('/seckill/submit', headers=auth(people[0]['token']), json={'product_id': pid, 'activity_id': aid})
        assert first.status_code in (200, 202), first.text
        notification = __import__('json').loads(await asyncio.wait_for(socket.recv(), 15))
        assert notification['type'] == 'order_created'
        assert notification['data']['price'] == '0.01'
    rest = await asyncio.gather(*[api.post('/seckill/submit', headers=auth(person['token']),
        json={'product_id': pid, 'activity_id': aid}) for person in people[1:]])
    responses = [first, *rest]
    accepted = [(person, response.json()['data']) for person, response in zip(people, responses) if response.status_code in (200, 202)]
    assert len(accepted) == 5
    assert sum(response.status_code == 409 for response in responses) == 3
    for person, data in accepted:
        created = await await_order(api, person['token'], data['request_id'])
        response = await api.get('/orders', headers=auth(person['token']))
        assert response.json()['data']['total'] == 1
        row = response.json()['data']['items'][0]
        assert row['id'] == created['order_id'] and row['price'] == '0.01'
    duplicate = await api.post('/seckill/submit', headers=auth(people[0]['token']), json={'product_id': pid, 'activity_id': aid})
    assert duplicate.status_code == 200
    assert duplicate.json()['data']['request_id'] == first.json()['data']['request_id']
    assert duplicate.json()['data']['duplicate'] is True
    stock = (await api.get(f'/seckill/stock/{pid}')).json()['data']
    assert stock['remaining_stock'] == 0
    observation = await api.get('/seckill/observe/overview', headers=auth(admin))
    assert observation.status_code == 200, observation.text
    queues = observation.json()['data']['queues']
    assert len(queues) == 3 and all(row['available'] for row in queues), queues
    observed = next(row for row in observation.json()['data']['activities'] if row['id'] == aid)
    assert observed['redis']['remaining_stock'] == 0 and observed['redis']['accepted_users'] == 5
    assert observed['orders_created'] == 5 and observed['initial_stock'] == 5
    request_observation = await api.get('/seckill/observe/requests/' + first.json()['data']['request_id'], headers=auth(admin))
    assert request_observation.status_code == 200
    assert request_observation.json()['data']['status'] == 'created'
    assert (await api.get('/seckill/observe/overview', headers=auth(people[1]['token']))).status_code == 403
    assert (await api.get('/seckill/requests/' + first.json()['data']['request_id'], headers=auth(people[1]['token']))).status_code == 404
    for _ in range(2):
        response = await api.post(f'/seckill/activities/{aid}/close', headers=auth(admin))
        assert response.status_code == 200 and response.json()['data']['released_stock'] == 0
    assert (await api.get(f'/products/{pid}')).json()['data']['stock'] == 1
    # The gateway must preserve validation and authorization status codes.
    assert (await api.post('/products', headers=auth(admin), json={'name': 'bad', 'price': 'NaN'})).status_code == 422
    assert (await api.post('/products', headers=auth(people[0]['token']), json={'name': 'bad', 'price': '1.00'})).status_code == 403
    response = await api.post('/ai/consult', headers=auth(people[0]['token']), json={'product_id': pid, 'question': '价格是多少？'})
    assert response.status_code == 200
    assert 'degraded' in response.json()['data']
    assert (await api.post('/logout', headers=auth(people[0]['token']))).status_code == 200
    assert (await api.get('/orders', headers=auth(people[0]['token']))).status_code == 401


async def test_websocket_cannot_subscribe_to_another_user(api):
    person = await customer(api)
    target = urlsplit(BASE_URL)
    url = f'ws://{target.netloc}/ws/{person["user"]["id"] + 1}'
    async with websockets.connect(url, open_timeout=5) as socket:
        await socket.send(__import__('json').dumps({'token': person['token']}))
        with pytest.raises(websockets.ConnectionClosed):
            await asyncio.wait_for(socket.recv(), 10)
