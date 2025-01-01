import os
from typing import cast
from eth_hash.auto import keccak
from eth_typing import ChecksumAddress

CACHE_SIZE = int(os.getenv("CHECKSUM_CACHE_SIZE", 1000))
CHECKSUM_CACHE = {}

def to_checksum_address(address: str) -> ChecksumAddress:
    """Fast checksum address with caching."""
    normalized_address = address.lower().replace("0x", "")
    assert len(normalized_address) == 40, "Address has incorrect length"

    if normalized_address in CHECKSUM_CACHE:
        return CHECKSUM_CACHE[normalized_address]

    hashed_address = keccak(normalized_address.encode()).hex()
    checksum_address = "0x" + "".join(
        char.upper() if int(hashed_address[i], 16) >= 8 else char
        for i, char in enumerate(normalized_address)
    )

    if len(CHECKSUM_CACHE) >= CACHE_SIZE:
        CHECKSUM_CACHE.pop(next(iter(CHECKSUM_CACHE)))

    CHECKSUM_CACHE[normalized_address] = checksum_address
    return cast(ChecksumAddress, checksum_address)