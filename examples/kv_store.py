import sys
import asyncio
from optparse import OptionParser

from rafter import TelnetService, SSHService
from rafter import RaftServer
from rafter.network import ZMQTransport


if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option("-b", action='store_true', dest="bootstrap", help="Set this flag if you are bootstrapping a cluster")

    (options, args) = parser.parse_args()
    server = RaftServer(TelnetService(), transport=ZMQTransport(), bootstrap=options.bootstrap)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.start())
    loop.run_forever()
