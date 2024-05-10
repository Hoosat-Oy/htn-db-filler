# encoding: utf-8
import asyncio

from htnd.HtndClient import HtndClient
# pipenv run python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/rpc.proto ./protos/messages.proto ./protos/p2p.proto
from htnd.HtndThread import HtndCommunicationError


class HtndMultiClient(object):
    def __init__(self, hosts: list[str]):
        self.htnds = [HtndClient(*h.split(":")) for h in hosts]

    def __get_htnd(self):
        for k in self.htnds:
            if k.is_utxo_indexed and k.is_synced:
                return k

    async def initialize_all(self):
        tasks = [asyncio.create_task(k.ping()) for k in self.htnds]

        for t in tasks:
            await t

    async def request(self, command, params=None, timeout=60, retry=3):
        try:
            return await self.__get_htnd().request(command, params, timeout=timeout, retry=1)
        except HtndCommunicationError:
            await self.initialize_all()
            return await self.__get_htnd().request(command, params, timeout=timeout, retry=retry)

    async def notify(self, command, params, callback):
        return await self.__get_htnd().notify(command, params, callback)
