import asyncio
import os
import uuid
import pytest
from common.mq import connect_mq, persistent_message, DEAD_QUEUE
from common.redis_client import redis_client
from common.seckill_store import request_key
from tests.test_live import admin_token, customer, setup_activity, await_order, auth

pytestmark = [pytest.mark.live, pytest.mark.skipif(os.getenv('RUN_LIVE_TESTS') != '1', reason='real broker not configured')]


async def test_real_broker_duplicate_delivery_and_poison_message(api):
    admin, person = await admin_token(api), await customer(api)
    pid, aid = await setup_activity(api, admin, stock=1)
    response = await api.post('/seckill/submit', headers=auth(person['token']), json={'product_id': pid, 'activity_id': aid})
    assert response.status_code in (200, 202), response.text
    rid = response.json()['data']['request_id']
    result = await await_order(api, person['token'], rid)
    body = await redis_client.hget(request_key(rid), 'data')
    connection, channel, exchange, _ = await connect_mq()
    try:
        for _ in range(10):
            await exchange.publish(persistent_message(body.encode(), rid), routing_key='order.create', mandatory=True)
        marker = str(uuid.uuid4())
        await exchange.publish(persistent_message(b'{invalid-json', marker), routing_key='order.create', mandatory=True)
        dead = await channel.get_queue(DEAD_QUEUE)
        message = await dead.get(timeout=10, fail=False)
        # basic.get doesn't wait for future messages: poll with a bounded deadline.
        deadline = asyncio.get_running_loop().time() + 10
        while message is None and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.2)
            message = await dead.get(timeout=2, fail=False)
        assert message is not None, 'Poison message was not preserved in dead queue'
        if message.message_id != marker:
            await message.nack(requeue=True)
            pytest.fail('Unexpected dead-letter content: run on an isolated test deployment')
        assert message.body == b'{invalid-json'
        await message.ack()
        # Wait until messages before the poison message have drained.
        await asyncio.sleep(0.5)
        response = await api.get('/orders', headers=auth(person['token']))
        assert response.json()['data']['total'] == 1
        assert response.json()['data']['items'][0]['id'] == result['order_id']
    finally:
        await connection.close()
        await redis_client.aclose()
