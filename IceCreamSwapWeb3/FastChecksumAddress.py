import os
from functools import lru_cache
from typing import cast
from eth_hash.auto import keccak
from eth_typing import ChecksumAddress

@lru_cache(maxsize=int(os.getenv("CHECKSUM_CACHE_SIZE", 16384)))
def to_checksum_address(address: str) -> ChecksumAddress:
    """Fast checksum address with caching."""
    normalized_address = address.lower().replace("0x", "")
    assert len(normalized_address) == 40, "Address has incorrect length"

    hashed_address = keccak(normalized_address.encode()).hex()
    checksum_address = "0x" + "".join(
        char.upper() if int(hashed_address[i], 16) >= 8 else char
        for i, char in enumerate(normalized_address)
    )

    return cast(ChecksumAddress, checksum_address)