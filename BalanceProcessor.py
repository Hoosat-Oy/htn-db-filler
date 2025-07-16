
import asyncio
import logging

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
                query = session.execute(
                    """
                    SELECT DISTINCT script_public_key_address 
                    FROM transactions_outputs 
                    WHERE script_public_key_address IS NOT NULL 
                    ORDER BY script_public_key_address
                    """
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


    async def update_balance_from_rpc(self, addresses: List[str], session, batch_size: int = 10) -> None:
        if not isinstance(addresses, list):
            _logger.error(f"Expected a list of addresses, got {type(addresses)}: {addresses}")
            return
        if not all(isinstance(addr, str) for addr in addresses):
            _logger.error(f"Invalid address types in batch: {[type(addr) for addr in addresses]}")
            return

        failed_addresses = []
        for i in range(0, len(addresses), batch_size):
            batch_addresses = addresses[i:i + batch_size]
            _logger.info(f"Processing batch {i // batch_size + 1} with {len(batch_addresses)} addresses")
            _logger.debug(f"Batch addresses: {batch_addresses}")

            try:
                existing_balances = {
                    b.script_public_key_address: b
                    for b in session.query(Balance).filter(
                        Balance.__table__.c.script_public_key_address.in_(batch_addresses)
                    ).all()
                }

                for address in batch_addresses:
                    try:
                        new_balance = await self._get_balance_from_rpc(address)
                        _logger.debug(f"Fetched balance {new_balance} for address {address}")
                        existing_balance = existing_balances.get(address)

                        if new_balance is not None and new_balance != 0:
                            if existing_balance:
                                existing_balance.balance = new_balance
                                _logger.debug(f"Updated balance for address {address} to {new_balance}")
                            else:
                                new_record = Balance(
                                    script_public_key_address=address,
                                    balance=new_balance
                                )
                                session.add(new_record)
                                _logger.debug(f"Created new balance record for address {address} with balance {new_balance}")
                        else:
                            if existing_balance:
                                session.delete(existing_balance)
                                _logger.debug(f"Deleted balance record for address {address} (balance is 0 or None)")
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
