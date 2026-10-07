"""Two-stage fault test: CI stops RabbitMQ, then starts it again. Only the isolated CI stack is used."""
import json
import os
import pytest
from common.redis_client import redis_client
from tests.test_live import api, admin_token, customer, setup_activity, await_order, auth

pytestmark = [pytest.mark.live, pytest.mark.skipif(os.getenv('RUN_LIVE_TESTS') != '1' or not os.getenv('BROKER_FAULT_STAGE'), reason='fault injection not requested')]


async def test_broker_outage_retains_and_recovers_accepted_work(api):
    stage = os.environ['BROKER_FAULT_STAGE']
    if stage == 'outage':
        admin, person = await admin_token(api), await customer(api)
        pid, aid = await setup_activity(api, admin, stock=1)
        response = await api.post('/seckill/submit', headers=auth(person['token']), json={'product_id': pid, 'activity_id': aid})
        assert response.status_code == 202, response.text
        rid = response.json()['data']['request_id']
        result = (await api.get('/seckill/requests/' + rid, headers=auth(person['token']))).json()['data']
        assert result['status'] == 'queued'
        assert (await api.get('/orders', headers=auth(person['token']))).json()['data']['total'] == 0
        await redis_client.set('tests:broker-outage', json.dumps({'token': person['token'], 'request_id': rid}), ex=600)
    elif stage == 'recovery':
        stored = await redis_client.get('tests:broker-outage')
        assert stored, 'Missing outage-stage evidence'
        record = json.loads(stored)
        result = await await_order(api, record['token'], record['request_id'], timeout=60)
        response = await api.get('/orders', headers=auth(record['token']))
        assert response.json()['data']['total'] == 1
        assert response.json()['data']['items'][0]['id'] == result['order_id']
        await redis_client.delete('tests:broker-outage')
    else:
        pytest.fail('Unknown fault stage')
    await redis_client.aclose()
