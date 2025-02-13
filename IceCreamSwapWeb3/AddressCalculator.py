from functools import lru_cache

import eth_utils
import rlp
from eth_utils import to_bytes
from web3 import Web3

@lru_cache(maxsize=1024)
def get_dict_slot(slot: int, key: int) -> int:
    return int.from_bytes(Web3.solidity_keccak(["uint256", "uint256"], [key, slot]), byteorder='big')


@lru_cache(maxsize=1024)
def calculate_create_address(sender: str, nonce: int) -> str:
    assert len(sender) == 42
    sender_bytes = to_bytes(hexstr=sender)
    raw = rlp.encode([sender_bytes, nonce])
    h = eth_utils.keccak(raw)
    address_bytes = h[12:]
    return eth_utils.to_checksum_address(address_bytes)