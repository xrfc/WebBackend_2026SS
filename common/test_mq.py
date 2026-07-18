"""测试模式共享内存MQ - seckill和order服务共享"""
import asyncio

_queue: asyncio.Queue = None


def get_queue() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()
    return _queue
