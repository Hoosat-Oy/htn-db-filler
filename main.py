import asyncio
import logging
import os

from BlocksProcessor import BlocksProcessor
from VirtualChainProcessor import VirtualChainProcessor
from dbsession import create_all, session_maker
from htnd.HtndMultiClient import HtndMultiClient
from models.Transaction import Transaction

logging.basicConfig(format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
                    level=logging.DEBUG if os.getenv("DEBUG", False) else logging.INFO,
                    handlers=[
                        logging.StreamHandler()
                    ]
                    )

# disable sqlalchemy notifications
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

# get file logger
_logger = logging.getLogger(__name__)

# create tables in database
create_all(drop=False)

htnd_hosts = []

for i in range(100):
    try:
        htnd_hosts.append(os.environ[f"HTND_HOST_{i + 1}"].strip())
    except KeyError:
        break

if not htnd_hosts:
    raise Exception('Please set at least HTND_HOST_1 environment variable.')

# create Kaspad client
client = HtndMultiClient(htnd_hosts)
task_runner = None


async def main():
    # initialize kaspads
    await client.initialize_all()

    # find last acceptedTx's block hash, when restarting this tool
    with session_maker() as s:
        try:
            start_hash = s.query(Transaction) \
                .where(Transaction.is_accepted == True) \
                .order_by(Transaction.block_time.desc()) \
                .limit(1) \
                .first() \
                .accepting_block_hash
        except AttributeError:
            start_hash = None

    # if there is nothing in the db, just get latest block.
    if not start_hash:
        daginfo = await client.request("getBlockDagInfoRequest", {})
        start_hash = daginfo["getBlockDagInfoResponse"]["pruningPointHash"]

    _logger.info(f"Start hash: {start_hash}")

    # create instances of blocksprocessor and virtualchainprocessor
    bp = BlocksProcessor(client)
    vcp = VirtualChainProcessor(client, start_hash)

    async def handle_blocks_commited(e):
        """
        this function is executed, when a new cluster of blocks were added to the database
        """
        global task_runner
        if task_runner and not task_runner.done():
            return

        _logger.debug('Update is_accepted for TXs.')
        task_runner = asyncio.create_task(vcp.yield_to_database())

    # set up event to fire after adding new blocks
    bp.on_commited += handle_blocks_commited

    # start blocks processor working concurrent
    while True:
        try:
            await bp.loop(start_hash)
        except Exception:
            _logger.exception('Exception occured and script crashed. Restart in 1m')
            bp.synced = False
            await asyncio.sleep(60)
            with session_maker() as s:
                start_hash = s.query(Transaction) \
                    .where(Transaction.is_accepted == True) \
                    .order_by(Transaction.block_time.desc()) \
                    .limit(1) \
                    .first() \
                    .accepting_block_hash


if __name__ == '__main__':
    asyncio.run(main())
