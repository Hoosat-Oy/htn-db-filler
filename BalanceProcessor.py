
import asyncio
import logging

import time
from typing import List
from dbsession import session_maker
from models.Balance import Balance
from sqlalchemy.exc import SQLAlchemyError

_logger = logging.getLogger(__name__)

class BalanceProcessor(object):

    def __init__(self, client):
        self.client = client
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
