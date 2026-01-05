
import asyncio
import logging
import os
import threading
import time
from typing import List
from dbsession import session_maker
from models.Balance import Balance
from sqlalchemy.exc import SQLAlchemyError

_logger = logging.getLogger(__name__)

class BalanceProcessor(object):

    def __init__(self, client):
        self.client = client

        self._pause_seconds = float(os.getenv("BALANCE_PAUSE_SECONDS", "0.05"))
        self._batch_size = int(os.getenv("BALANCE_BATCH_SIZE", "25"))
        self._threaded = os.getenv("BALANCE_THREADED", "True").lower() in ["true", "1", "t", "y", "yes"]

        self._pending_lock = threading.Lock()
        self._pending_addrs: set[str] = set()
        self._has_work = threading.Event()
        self._stop = threading.Event()
        self._worker_thread: threading.Thread | None = None

        if self._threaded:
            self.start_worker()

    def start_worker(self) -> None:
        if self._worker_thread and self._worker_thread.is_alive():
            return
        self._stop.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True, name="BalanceProcessor")
        self._worker_thread.start()

    def stop_worker(self, join_timeout: float | None = 2.0) -> None:
        self._stop.set()
        self._has_work.set()
        if self._worker_thread and join_timeout is not None:
            self._worker_thread.join(timeout=join_timeout)

    def enqueue_balance_updates(self, addresses: List[str] | None) -> None:
        """Queue addresses for balance refresh without blocking the event loop."""
        if not addresses:
            return
        if not isinstance(addresses, list):
            _logger.error(f"enqueue_balance_updates: expected list, got {type(addresses)}")
            return
        with self._pending_lock:
            for addr in addresses:
                if isinstance(addr, str) and addr:
                    self._pending_addrs.add(addr)
        self._has_work.set()

    def _drain_pending_batch(self, max_items: int) -> list[str]:
        batch: list[str] = []
        with self._pending_lock:
            while self._pending_addrs and len(batch) < max_items:
                batch.append(self._pending_addrs.pop())
            if not self._pending_addrs:
                self._has_work.clear()
        return batch

    def _worker_loop(self) -> None:
        """Background thread loop: fetch balances with pause and write to DB.

        This intentionally runs in a separate OS thread because SQLAlchemy calls are
        synchronous and would otherwise block the asyncio event loop.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while not self._stop.is_set():
                self._has_work.wait(timeout=1.0)
                if self._stop.is_set():
                    break

                batch = self._drain_pending_batch(self._batch_size)
                if not batch:
                    continue

                failed_addresses: list[str] = []
                with session_maker() as session:
                    for address in batch:
                        if self._stop.is_set():
                            break
                        try:
                            new_balance = loop.run_until_complete(self._get_balance_from_rpc(address))
                            _logger.debug(f"Fetched balance {new_balance} for address {address}")

                            existing_balance = session.query(Balance).filter_by(script_public_key_address=address).first()
                            if new_balance is None:
                                if existing_balance:
                                    session.delete(existing_balance)
                            else:
                                if existing_balance:
                                    existing_balance.balance = new_balance
                                else:
                                    session.add(Balance(script_public_key_address=address, balance=new_balance))

                        except Exception as e:
                            _logger.error(f"Balance worker error for address {address}: {e}")
                            failed_addresses.append(address)

                        if self._pause_seconds > 0:
                            time.sleep(self._pause_seconds)

                    try:
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        _logger.error(f"Balance worker DB commit error: {e}")
                        # Re-queue the whole batch on commit failure
                        failed_addresses = list(set(failed_addresses + batch))

                if failed_addresses:
                    # Re-queue failures for retry
                    self.enqueue_balance_updates(failed_addresses)
        finally:
            try:
                loop.stop()
            except Exception:
                pass
            loop.close()

    async def _get_balance_from_rpc(self, address):
        """
        Fetch balance for the given address from the RPC node.
        """
        try:
            response = await self.client.request("getBalanceByAddressRequest", params= {"address": address}, timeout=60)

            get_balance_response = response.get("getBalanceByAddressResponse", {})
            balance = get_balance_response.get("balance", None)
            error = get_balance_response.get("error", None)

            if error:
                _logger.error(f"Error fetching balance for address {address}: {error}")
            
            if balance is not None:
                return int(balance)
            
            return None
        
        except Exception as e:
            _logger.error(f"Error fetching balance for address {address}: {e}")
            return None

    async def update_all_balances(self):
        with session_maker() as session:
            try:
                from sqlalchemy import text
                query = session.execute(
                    text("""
                    SELECT DISTINCT script_public_key_address 
                    FROM transactions_outputs 
                    WHERE script_public_key_address IS NOT NULL 
                    ORDER BY script_public_key_address
                    """)
                )

                addresses = [row[0] for row in query.fetchall()]

                if not addresses:
                    _logger.info("No addresses found to update balances.")
                    return

                _logger.info(f"Found {len(addresses)} addresses to update balances.")

                # For bulk updates (e.g. on boot), allow threading to avoid blocking.
                if self._threaded:
                    self.enqueue_balance_updates(addresses)
                else:
                    await self.update_balance_from_rpc(addresses, 10)
                    await asyncio.sleep(0.1)

            except Exception as e:
                _logger.error(f"Error updating balances: {e}")
                return


    async def update_balance_from_rpc(self, addresses: List[str], batch_size: int = 10) -> None:
        _logger.info(
            f"update_balance_from_rpc: received "
            f"{len(addresses) if isinstance(addresses, list) else 'non-list'} "
            f"addresses, batch_size={batch_size}"
        )

        if not isinstance(addresses, list):
            _logger.error(f"Expected a list of addresses, got {type(addresses)}: {addresses}")
            return
        if len(addresses) == 0:
            _logger.info("update_balance_from_rpc: no addresses to update, skipping.")
            return
        if not all(isinstance(addr, str) for addr in addresses):
            _logger.error(f"Invalid address types in batch: {[type(addr) for addr in addresses]}")
            return

        failed_addresses = []
        created_count = 0
        updated_count = 0
        deleted_count = 0
        processed_count = 0
        skipped_count = 0
        with session_maker() as session:
            for i in range(0, len(addresses), batch_size):
                batch_addresses = addresses[i:i + batch_size]
                _logger.info(f"Processing batch {i // batch_size + 1} with {len(batch_addresses)} addresses")
                _logger.debug(f"Batch addresses: {batch_addresses}")

                try:
                    for address in batch_addresses:
                        try:
                            new_balance = await self._get_balance_from_rpc(address)
                            _logger.debug(f"Fetched balance {new_balance} for address {address}")
                            existing_balance = session.query(Balance).filter_by(script_public_key_address=address).first()

                            if new_balance is None:
                                if existing_balance:
                                    session.delete(existing_balance)
                                    _logger.debug(f"Deleted balance record for address {address} (balance is None)")
                                    deleted_count += 1
                                else:
                                    skipped_count += 1
                            else:
                                if existing_balance:
                                    existing_balance.balance = new_balance
                                    _logger.debug(f"Updated balance for address {address} to {new_balance}")
                                    updated_count += 1
                                else:
                                    new_record = Balance(
                                        script_public_key_address=address,
                                        balance=new_balance
                                    )
                                    session.add(new_record)
                                    _logger.debug(f"Created new balance record for address {address} with balance {new_balance}")
                                    created_count += 1
                            processed_count += 1
                        except Exception as e:
                            _logger.error(f"Error processing address {address} in batch {i // batch_size + 1}: {e}")
                            failed_addresses.append(address)
                            continue

                    session.commit()
                    _logger.info(f"Committed batch {i // batch_size + 1} with {len(batch_addresses)} addresses")
                except SQLAlchemyError as db_err:
                    _logger.error(f"Database error in batch {i // batch_size + 1}: {db_err}")
                    session.rollback()
                    failed_addresses.extend(batch_addresses)
                    continue
                except Exception as e:
                    _logger.error(f"Unexpected error in batch {i // batch_size + 1}: {e}")
                    session.rollback()
                    failed_addresses.extend(batch_addresses)
                    continue

        if failed_addresses:
            _logger.warning(f"Failed to process {len(failed_addresses)} addresses: {failed_addresses[:10]}{'...' if len(failed_addresses) > 10 else ''}")
        else:
            _logger.info(f"Successfully processed all {len(addresses)} addresses")

        _logger.info(
            f"update_balance_from_rpc summary: processed={processed_count}, "
            f"created={created_count}, updated={updated_count}, deleted={deleted_count}, "
            f"skipped={skipped_count}, failed={len(failed_addresses)}"
        )
