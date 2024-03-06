# encoding: utf-8
import asyncio

from htnd.HtndClient import HtndClient
# pipenv run python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/rpc.proto ./protos/messages.proto ./protos/p2p.proto
from htnd.HtndThread import HtndCommunicationError


class HtndMultiClient(object):
    def __init__(self, hosts):
        self.htnpads = [HtndClient(*h.split(":")) for h in hosts]

    def __get_htnpad(self):
        for k in self.htnpads:
            if k.is_utxo_indexed and k.is_synced:
                return k

    async def initialize_all(self):
        tasks = [asyncio.create_task(k.ping()) for k in self.htnpads]

        for t in tasks:
            await t

    async def request(self, command, params=None, timeout=5):
        try:
            return await self.__get_htnpad().request(command, params, timeout=timeout)
        except HtndCommunicationError:
            await self.initialize_all()
            return await self.__get_htnpad().request(command, params, timeout=timeout)

    async def notify(self, command, params, callback):
        return await self.__get_htnpad().notify(command, params, callback)
