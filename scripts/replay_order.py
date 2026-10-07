"""Replay one inspected dead-letter event with the original idempotency ID."""
import argparse
import asyncio
from uuid import UUID
from common.events import OrderEvent
from common.mq import connect_mq, persistent_message, DEAD_QUEUE


async def replay(request_id):
    connection, channel, exchange, _ = await connect_mq()
    held = []
    try:
        queue = await channel.get_queue(DEAD_QUEUE)
        for _ in range(100):
            message = await queue.get(fail=False, timeout=3)
            if message is None:
                break
            held.append(message)
            try:
                event = OrderEvent.model_validate_json(message.body)
            except ValueError:
                continue
            if str(event.request_id) != request_id:
                continue
            await exchange.publish(persistent_message(message.body, request_id, {'attempts': 0}),
                routing_key='order.create', mandatory=True)
            await message.ack()
            held.remove(message)
            print('Replay confirmed. Query the request status to verify the final SQL order.')
            return
        raise RuntimeError('No matching valid event in the first 100 dead letters; no messages deleted')
    finally:
        for message in held:
            if not message.processed:
                await message.nack(requeue=True)
        await connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Replay one event after fixing its root cause')
    parser.add_argument('--request-id', required=True, type=UUID)
    args = parser.parse_args()
    asyncio.run(replay(str(args.request_id)))
