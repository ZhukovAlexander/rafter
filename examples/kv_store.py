import asyncio

from rafter.service import JsonRPCService
from rafter.server import RaftServer


if __name__ == '__main__':
    server = RaftServer(JsonRPCService)
    server.start()
    asyncio.get_event_loop().run_forever()
