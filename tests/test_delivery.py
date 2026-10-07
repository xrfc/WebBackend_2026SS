import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from sqlalchemy.exc import OperationalError
from common.events import OrderEvent
from common.mq import DEAD_QUEUE, RETRY_QUEUE
from common.seckill_store import OUTBOX, OUTBOX_GROUP, reserve, read_request
from services.order_service import main as order
from services.seckill_service import main as seckill
from tests.test_boundaries import activity


def message(body, attempts=0):
    return SimpleNamespace(body=body, headers={'attempts': attempts}, message_id='stable-id',
        ack=AsyncMock(), nack=AsyncMock())


def consumer():
    worker = order.MQConsumer()
    worker.channel = SimpleNamespace(default_exchange=SimpleNamespace(publish=AsyncMock()))
    return worker


async def real_message(tokens):
    _, aid = await activity(tokens)
    rid = str(uuid.uuid4())
    await reserve(aid, 2, rid)
    record = await read_request(rid)
    return message(json.dumps({k: v for k, v in record.items() if k != 'state'}).encode())


async def test_transient_db_failure_is_retried_before_ack(tokens, monkeypatch):
    msg = await real_message(tokens)
    worker = consumer()
    monkeypatch.setattr(order, 'create_order', AsyncMock(side_effect=OperationalError('query', {}, Exception('unavailable'))))
    await worker.handle_message(msg)
    call = worker.channel.default_exchange.publish.call_args
    assert call.kwargs['routing_key'] == RETRY_QUEUE
    assert call.args[0].headers['attempts'] == 1
    msg.ack.assert_awaited_once()
    msg.nack.assert_not_awaited()


async def test_exhausted_retries_are_preserved_for_review(tokens, monkeypatch):
    msg = await real_message(tokens)
    msg.headers['attempts'] = 3
    worker = consumer()
    monkeypatch.setattr(order, 'create_order', AsyncMock(side_effect=OperationalError('query', {}, Exception('down'))))
    await worker.handle_message(msg)
    assert worker.channel.default_exchange.publish.call_args.kwargs['routing_key'] == DEAD_QUEUE
    msg.ack.assert_awaited_once()
    record = await read_request(json.loads(msg.body)['request_id'])
    assert record['state'] == 'manual_review'


async def test_poison_message_goes_to_dead_queue():
    msg, worker = message(b'{invalid-json'), consumer()
    await worker.handle_message(msg)
    assert worker.channel.default_exchange.publish.call_args.kwargs['routing_key'] == DEAD_QUEUE
    msg.ack.assert_awaited_once()


async def test_failed_retry_publish_keeps_original_message(tokens, monkeypatch):
    msg, worker = await real_message(tokens), consumer()
    monkeypatch.setattr(order, 'create_order', AsyncMock(side_effect=OperationalError('query', {}, Exception('down'))))
    worker.channel.default_exchange.publish.side_effect = ConnectionError('broker down')
    monkeypatch.setattr(order.asyncio, 'sleep', AsyncMock())
    await worker.handle_message(msg)
    msg.ack.assert_not_awaited()
    msg.nack.assert_awaited_once_with(requeue=True)


async def test_committed_order_is_acked_even_if_push_fails(tokens, monkeypatch):
    msg, worker = await real_message(tokens), consumer()
    monkeypatch.setattr(order.redis_client, 'publish', AsyncMock(side_effect=ConnectionError('notifications down')))
    await worker.handle_message(msg)
    msg.ack.assert_awaited_once()
    worker.channel.default_exchange.publish.assert_not_awaited()
    assert (await seckill.request_result(json.loads(msg.body)['request_id'], 2))['status'] == 'created'


async def test_relay_failure_retains_pending_stream_entry(tokens, isolated_state):
    await isolated_state.xgroup_create(OUTBOX, OUTBOX_GROUP, id='0', mkstream=True)
    _, aid = await activity(tokens)
    rid = str(uuid.uuid4())
    await reserve(aid, 2, rid)
    streams = await isolated_state.xreadgroup(OUTBOX_GROUP, 'test-relay', streams={OUTBOX: '>'})
    relay = seckill.OutboxRelay()
    relay.exchange = SimpleNamespace(publish=AsyncMock(side_effect=ConnectionError('MQ down')))
    with pytest.raises(ConnectionError):
        await relay.publish_entries(streams[0][1])
    assert await isolated_state.xlen(OUTBOX) == 1
    assert (await isolated_state.xpending(OUTBOX, OUTBOX_GROUP))['pending'] == 1
    relay.exchange.publish.side_effect = None
    await relay.publish_entries(streams[0][1])
    assert relay.exchange.publish.call_args.args[0].message_id == rid
    assert await isolated_state.xlen(OUTBOX) == 0
    assert (await isolated_state.xpending(OUTBOX, OUTBOX_GROUP))['pending'] == 0
