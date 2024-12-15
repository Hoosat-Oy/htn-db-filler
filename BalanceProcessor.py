
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
            response = await self.client.request("getBalanceByAddressRequest", {"address": address})

            get_balance_response = response.get("getBalanceByAddressResponse", {})
            balance = get_balance_response.get("balance", 0)
            error = get_balance_response.get("error", None)

            if error:
                _logger.error(f"Error fetching balance for address {address}: {error}")
                return 0
            
            if balance is not None:
                return int(balance)
            
            _logger.error(f"Balance not found for address {address}: {response}")
            return 0
        
        except Exception as e:
            _logger.error(f"Error fetching balance for address {address}: {e}")
            return 0

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

                for address in addresses:
                    await self.update_balance_from_rpc(address)
                    await asyncio.sleep(1)  

            except Exception as e:
                _logger.error(f"Error updating balances: {e}")
                return


    async def update_balance_from_rpc(self, address):
        with session_maker() as session:
            try:
                balance = session.query(Balance).filter(Balance.script_public_key_address == address).first()
                address_balance = await self._get_balance_from_rpc(address) 
                _logger.debug(f"Updating address {address} balance to {address_balance}")

                if address_balance is None:
                    return
                
                if balance: 
                    balance.balance = address_balance
                else: 
                    balance = Balance(script_public_key_address=address, balance=address_balance)
                    session.add(balance)

                session.commit() 
            except Exception as e:
                _logger.error(f"Error fetching balance from the node for address {address}: {e}")
                return 