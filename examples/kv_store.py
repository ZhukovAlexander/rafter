import asyncio
from argparse import ArgumentParser

from rafter import TelnetService
from rafter import RaftServer, RaftLog, PersistentDict
from rafter.network import ZMQTransport

parser = ArgumentParser()
parser.add_argument("-b", action='store_true', dest="bootstrap", help="Set this flag if you are bootstrapping a cluster")
parser.add_argument("--service-port", type=int, dest="service_port", help="Port to bind a service to")
parser.add_argument("--port", type=int, dest="port", default=9999, help="Server port")
parser.add_argument("--path", type=str, dest="db_path", default='/tmp/rafter-0.lmdb', help="LMDB database path")
parser.add_argument("--connect", type=str, dest="connect", default=None, help="List of comma separeted peers to connect to")

if __name__ == '__main__':

    args = parser.parse_args()

    server = RaftServer(TelnetService(port=args.service_port),
                        log=RaftLog(args.db_path),
                        storage=PersistentDict(args.db_path),
                        transport=ZMQTransport(port=args.port, connect=args.connect.split(',') if args.connect is not None else None),
                        bootstrap=args.bootstrap)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.start())
    loop.run_forever()
