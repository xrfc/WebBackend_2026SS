import aio_pika
from common.config import settings

EXCHANGE = 'seckill_exchange'
ORDER_QUEUE = 'order_create_queue'
RETRY_QUEUE = 'order_retry_queue'
DEAD_QUEUE = 'order_dead_queue'


async def connect_mq():
    connection = await aio_pika.connect_robust(host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT, login=settings.RABBITMQ_USER,
        password=settings.RABBITMQ_PASS, timeout=3)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=settings.MQ_PREFETCH)
        exchange = await channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue(ORDER_QUEUE, durable=True)
        await queue.bind(exchange, routing_key='order.create')
        await channel.declare_queue(RETRY_QUEUE, durable=True, arguments={
            'x-message-ttl': settings.MQ_RETRY_DELAY_MS,
            'x-dead-letter-exchange': EXCHANGE,
            'x-dead-letter-routing-key': 'order.create',
        })
        await channel.declare_queue(DEAD_QUEUE, durable=True)
        return connection, channel, exchange, queue
    except BaseException:
        await connection.close()
        raise


def persistent_message(body: bytes, request_id: str, headers=None):
    return aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=request_id, content_type='application/json', headers=headers or {})
