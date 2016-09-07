import sys
import asyncio
from optparse import OptionParser

from rafter.service import JsonRPCService
from rafter.server import RaftServer


if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option("-b", action='store_true', dest="bootstrap", help="write report to FILE")

    (options, args) = parser.parse_args()
    server = RaftServer(JsonRPCService, bootstrap=options.bootstrap)
    server.start()
    asyncio.get_event_loop().run_forever()
