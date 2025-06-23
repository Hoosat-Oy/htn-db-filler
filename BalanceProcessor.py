
import asyncio
import logging

from dbsession import session_maker
from models.Balance import Balance

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
                return None
            
            if balance is not None:
                return int(balance)
            
            _logger.error(f"Balance not found for address {address}: {response}")
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

                
                await self.update_balance_from_rpc(addresses)
                await asyncio.sleep(0.1)  

            except Exception as e:
                _logger.error(f"Error updating balances: {e}")
                return


    async def update_balance_from_rpc(self, addresses):
        with session_maker() as session:
            existing_balances = {
                b.script_public_key_address: b
                for b in session.query(Balance).filter(Balance.script_public_key_address.in_(addresses)).all()
            }

            for address, existing_balance in existing_balances.items():
                try:
                    if existing_balances is not None:
                        new_balance = await self._get_balance_from_rpc(address)
                        _logger.debug(f"Updating address {address} balance to {new_balance}")

                        if new_balance is None or new_balance == 0:
                            session.delete(existing_balance)
                            _logger.info(f"Deleted balance record for address {address} as balance is 0.")
                        else:
                            existing_balance.balance = new_balance
                except Exception as e:
                    _logger.error(f"Error updating balance for address {address}: {e}")
            session.commit()
