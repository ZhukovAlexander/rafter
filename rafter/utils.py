import collections
import asyncio


class AsyncDictWrapper:

    def __init__(self, d, loop=None):
        self._d = d
        self._loop = loop or asyncio.get_event_loop()
        self._waiters = collections.defaultdict(lambda: asyncio.Condition(loop=self._loop))

    async def wait_for(self, key):
        try:
            return self._d[key]
        except KeyError:
            c = self._waiters[key]
            async with c:
                return await c.wait_for(lambda: self._d.get(key))

    async def set(self, key, value):
        c = self._waiters[key]
        async with c:
            self._d[key] = value
            c.notify_all()