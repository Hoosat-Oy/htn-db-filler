
import asyncio
import logging
import sys
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from dbsession import session_maker
from models.Block import Block
from models.Transaction import Transaction, TransactionOutput, TransactionInput
from utils.Event import Event

_logger = logging.getLogger(__name__)

CLUSTER_SIZE = 150
CLUSTER_WAIT_SECONDS = 190
B_TREE_SIZE = 2500

task_runner = None

class BlocksProcessor(object):
    """
    BlocksProcessor polls hoosat for blocks and adds the meta information and it's transactions into database.
    """

    def __init__(self, client, vcp_instance, balance, batch_processing = False, env_start_hash = None, env_enable_balance = False):
        self.client = client
        self.blocks_to_add = []
        self.balance = balance
        self.addresses_to_update = []

        self.txs = {}
        self.txs_output = []
        self.txs_input = []
        self.vcp = vcp_instance
        self.env_start_hash = env_start_hash
        self.env_enable_balance = env_enable_balance
        self.batch_processing = batch_processing

        # Did the loop already see the DAG tip
        self.tipFound = False

    async def loop(self, start_point):
        # go through each block added to DAG
        _logger.info('Start processing blocks from %s', start_point)
        blocks = []
        async for block_hash, block in self.blockiter(start_point):
            blocks.append(block)
            # prepare add block and tx to database
            await self.__add_block_to_queue(block_hash, block)
            await self.__add_tx_to_queue(block_hash, block)
            # if cluster size is reached, insert to database
            if len(self.blocks_to_add) >= CLUSTER_SIZE:
                await self.commit_blocks()
                if self.env_enable_balance != False:
                   await self.commit_balances()
                if self.batch_processing == False:
                    await self.commit_txs()
                else: 
                    await self.batch_commit_txs()
                asyncio.create_task(self.handle_blocks_committed(blocks))

    async def commit_balances(self):
        unique_addresses = list(set(self.addresses_to_update))
        for address in unique_addresses:    
            await self.balance.update_balance_from_rpc(address)
            await asyncio.sleep(0.1)  
        self.addresses_to_update = []

    async def handle_blocks_committed(self, blocks):
        """
        this function is executed, when a new cluster of blocks were added to the database
        """
        global task_runner
        for block in blocks:
            is_chain_block = block["verboseData"].get("isChainBlock", False)
            if is_chain_block is True: 
                _logger.info(f'Starting VCP for {block["verboseData"]["hash"]} {is_chain_block}')
                while task_runner and not task_runner.done():
                    await task_runner
                task_runner = asyncio.create_task(self.vcp.yield_to_database(block["verboseData"]["hash"]))
        blocks = []

    async def blockiter(self, start_point):
        """
        generator for iterating the blocks added to the blockDAG
        """
        low_hash = start_point
        while True:
            daginfo = await self.client.request("getBlockDagInfoRequest", {})
            resp = await self.client.request("getBlocksRequest",
                                             params={
                                                 "lowHash": low_hash,
                                                 "includeTransactions": True,
                                                 "includeBlocks": True
                                             },
                                             timeout=60)
            # go through each block and yield
            for i, blockHash in enumerate(resp["getBlocksResponse"].get("blockHashes", [])):
                if daginfo["getBlockDagInfoResponse"]["tipHashes"][0] == blockHash:
                    _logger.info(f'Found tip hash {daginfo["getBlockDagInfoResponse"]["tipHashes"][0]}')
                    self.tipFound = True
                    break
                # ignore the first block, which is not start point. It is already processed by previous request
                if blockHash == low_hash and blockHash != start_point:
                    continue
                # yield blockhash and it's data
                yield blockHash, resp["getBlocksResponse"]["blocks"][i]

            # if synced, poll blocks after 1s
            if self.tipFound:
                low_hash = daginfo["getBlockDagInfoResponse"]["tipHashes"][0]
                _logger.debug(f'Waiting for the next blocks request, low hash {low_hash}')
                self.tipFound = False
                await asyncio.sleep(CLUSTER_WAIT_SECONDS)
            else: 
                if len(resp["getBlocksResponse"].get("blockHashes", [])) > 1:
                    low_hash = resp["getBlocksResponse"]["blockHashes"][-1]
                else:
                    _logger.debug('')
                    await asyncio.sleep(2)

    async def __add_tx_to_queue(self, block_hash, block):
        """
        Adds block's transactions to queue. This is only prepartion without commit!
        """
        
        if block.get("transactions") is not None:
            for transaction in block["transactions"]:
                if transaction.get("verboseData") is not None:
                    tx_id = transaction["verboseData"]["transactionId"]

                    # Check, that the transaction isn't prepared yet. Otherwise ignore
                    if not self.is_tx_id_in_queue(tx_id):
                        # Add transaction
                        self.txs[tx_id] = Transaction(subnetwork_id=transaction["subnetworkId"],
                                                    transaction_id=tx_id,
                                                    hash=transaction["verboseData"]["hash"],
                                                    mass=transaction["verboseData"].get("mass"),
                                                    block_hash=[transaction["verboseData"]["blockHash"]],
                                                    block_time=int(transaction["verboseData"]["blockTime"]))
                        # Track unique addresses to prevent duplicates
                        # Process the outputs (increase balance)
                        for index, out in enumerate(transaction.get("outputs", [])):
                            address = out["verboseData"]["scriptPublicKeyAddress"]
                            amount = out["amount"]
                            self.addresses_to_update.append(address)

                            self.txs_output.append(TransactionOutput(transaction_id=tx_id,
                                                                    index=index,
                                                                    amount=amount,
                                                                    script_public_key=out["scriptPublicKey"]["scriptPublicKey"],
                                                                    script_public_key_address=address,
                                                                    script_public_key_type=out["verboseData"]["scriptPublicKeyType"]))

                        # Process the inputs (decrease balance)
                        for index, tx_in in enumerate(transaction.get("inputs", [])):
                            if self.env_enable_balance != False: 
                                prev_out_tx_id = tx_in["previousOutpoint"]["transactionId"]
                                prev_out_index = int(tx_in["previousOutpoint"].get("index", 0))

                                # Query the previous transaction output (this is where the input came from)
                                with session_maker() as session:
                                    prev_output = session.query(TransactionOutput).filter_by(
                                        transaction_id=prev_out_tx_id,
                                        index=prev_out_index
                                    ).first()

                                    if prev_output:
                                        address = prev_output.script_public_key_address
                                        self.addresses_to_update.append(address)

                            self.txs_input.append(TransactionInput(transaction_id=tx_id,
                                                                    index=index,
                                                                    previous_outpoint_hash=prev_out_tx_id,
                                                                    previous_outpoint_index=prev_out_index,
                                                                    signature_script=tx_in["signatureScript"],
                                                                    sig_op_count=tx_in.get("sigOpCount", 0)))
                    else:
                        # If the block if already in the Queue, merge the block_hashes.
                        self.txs[tx_id].block_hash = list(set(self.txs[tx_id].block_hash + [block_hash]))

    async def batch_commit_txs(self):
        """
        Add all queued transactions and its in- and outputs to the database in batches to avoid exceeding PostgreSQL limits.
        """
        BATCH_SIZE = 5  # Define a suitable batch size

        # First, handle updates for existing transactions.
        tx_ids_to_add = list(self.txs.keys())

        # Calculate the number of batches needed
        num_batches = len(tx_ids_to_add) // BATCH_SIZE + (1 if len(tx_ids_to_add) % BATCH_SIZE > 0 else 0)

        for i in range(num_batches):
            with session_maker() as session:
                # Determine the subset of transaction IDs for this batch
                batch_tx_ids = tx_ids_to_add[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]

                # Query only the transactions in the current batch
                tx_items = session.query(Transaction).filter(Transaction.transaction_id.in_(batch_tx_ids)).all()

                for tx_item in tx_items:
                    # Update block_hash by combining existing ones with new ones from self.txs
                    new_block_hashes = list(set(tx_item.block_hash) | set(self.txs[tx_item.transaction_id].block_hash))
                    tx_item.block_hash = new_block_hashes
                    # Remove the transaction from self.txs since it's now been processed
                    self.txs.pop(tx_item.transaction_id)

                # Commit the updates for this batch
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    _logger.error(f'Error updating transactions in batch {i+1}/{num_batches}: {e}')

        # Pre-map outputs and inputs to their transaction IDs
        outputs_by_tx = {tx_id: [] for tx_id in self.txs.keys()}
        for output in self.txs_output:
            if output.transaction_id in outputs_by_tx:
                outputs_by_tx[output.transaction_id].append(output)

        inputs_by_tx = {tx_id: [] for tx_id in self.txs.keys()}
        for input in self.txs_input:
            if input.transaction_id in inputs_by_tx:
                inputs_by_tx[input.transaction_id].append(input)

        # Now, handle insertion of new transactions in batches.
        all_new_txs = list(self.txs.values())

        for i in range(0, len(all_new_txs), BATCH_SIZE):
            with session_maker() as session:
                batch_txs = all_new_txs[i:i + BATCH_SIZE]
                batch_tx_ids = [tx.transaction_id for tx in batch_txs]

                batch_outputs = [output for tx_id in batch_tx_ids for output in outputs_by_tx[tx_id]]
                batch_inputs = [input for tx_id in batch_tx_ids for input in inputs_by_tx[tx_id]]

                session.add_all(batch_txs)
                session.add_all(batch_outputs)
                session.add_all(batch_inputs)

                try:
                    session.commit()
                    # _logger.info(f'Added {len(batch_txs)} TXs to database in a batch.')
                except Exception as e:
                    session.rollback()
                    _logger.error(f'Error adding TXs to database in a batch {i+1}/{num_batches}: {e}')
                    # Further error handling could be implemented here as needed.

        # Reset queues after all batches have been processed.
        self.txs = {}
        self.txs_input = []
        self.txs_output = []

    # Original commit_txs
    async def commit_txs(self):
        """
        Add all queued transactions and it's in- and outputs to database
        """
        # First go through all transactions and check, if there are already added ones.
        # If yes, update block_hash and remove from queue
        tx_ids_to_add = list(self.txs.keys())
        with session_maker() as session:
            tx_items = session.query(Transaction).filter(Transaction.transaction_id.in_(tx_ids_to_add)).all()
            for tx_item in tx_items:
                tx_item.block_hash = list((set(tx_item.block_hash) | set(self.txs[tx_item.transaction_id].block_hash)))
                self.txs.pop(tx_item.transaction_id)
            session.commit()

        # Go through all transactions which were not in the database and add now.
        with session_maker() as session:
            # go through queues and add
            for _ in self.txs.values():
                session.add(_)

            for tx_output in self.txs_output:
                if tx_output.transaction_id in self.txs:
                    session.add(tx_output)

            for tx_input in self.txs_input:
                if tx_input.transaction_id in self.txs:
                    session.add(tx_input)

            try:
                session.commit()

                # reset queues
                self.txs = {}
                self.txs_input = []
                self.txs_output = []

            except IntegrityError:
                session.rollback()
                _logger.error(f'Error adding TXs to database')
                raise


    async def __add_block_to_queue(self, block_hash, block):
        """
        Adds a block to the queue, which is used for adding a cluster
        """

        if 'parents' in block["header"] and block["header"]["parents"]:
            parent_hashes = block["header"]["parents"][0].get("parentHashes", [])
        else:
            parent_hashes = []

        block_entity = Block(hash=block_hash,
                             accepted_id_merkle_root=block["header"]["acceptedIdMerkleRoot"],
                             difficulty=block["verboseData"]["difficulty"],
                             is_chain_block=block["verboseData"].get("isChainBlock", False),
                             merge_set_blues_hashes=block["verboseData"].get("mergeSetBluesHashes", []),
                             merge_set_reds_hashes=block["verboseData"].get("mergeSetRedsHashes", []),
                             selected_parent_hash=block["verboseData"]["selectedParentHash"],
                             bits=block["header"]["bits"],
                             blue_score=int(block["header"].get("blueScore", 0)),
                             blue_work=block["header"]["blueWork"],
                             daa_score=int(block["header"].get("daaScore", 0)),
                             hash_merkle_root=block["header"]["hashMerkleRoot"],
                             nonce=block["header"]["nonce"],
                             parents=parent_hashes,
                             pruning_point=block["header"]["pruningPoint"],
                             timestamp=datetime.fromtimestamp(int(block["header"]["timestamp"]) / 1000).isoformat(),
                             utxo_commitment=block["header"]["utxoCommitment"],
                             version=block["header"].get("version", 0))

        # remove same block hash
        self.blocks_to_add = [b for b in self.blocks_to_add if b.hash != block_hash]
        self.blocks_to_add.append(block_entity)

    async def commit_blocks(self):
        """
        Insert queued blocks to database
        """
        
        # delete already set old blocks
        with session_maker() as session:
            d = session.query(Block).filter(
                Block.hash.in_([b.hash for b in self.blocks_to_add])).delete()
            session.commit()

        # insert blocks
        with session_maker() as session:
            for _ in self.blocks_to_add:
                session.add(_)
            try:
                session.commit()
                # reset queue
                self.blocks_to_add = []
            except IntegrityError:
                session.rollback()
                _logger.error('Error adding group of blocks')
                raise
        

    def is_tx_id_in_queue(self, tx_id):
        """
        Checks if given TX ID is already in the queue
        """
        return tx_id in self.txs