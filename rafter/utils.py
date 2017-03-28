# Copyright 2017 Alexander Zhukov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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