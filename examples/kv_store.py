import sys
import asyncio
from optparse import OptionParser

from rafter.service import TelnetService
from rafter.server import RaftServer


if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option("-b", action='store_true', dest="bootstrap", help="Set this flag if you are bootstrapping a cluster")

    (options, args) = parser.parse_args()
    server = RaftServer(TelnetService(), bootstrap=options.bootstrap)
    server.start()
    asyncio.get_event_loop().run_forever()
