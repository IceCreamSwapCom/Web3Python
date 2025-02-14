import os
from functools import lru_cache
from typing import Type

from hexbytes import HexBytes as _HexBytes
from hexbytes.main import BytesLike


class HexBytes(_HexBytes):
    @lru_cache(maxsize=int(os.getenv("HEX_BYTES_CACHE_SIZE", 16384)))
    def __new__(cls: Type[bytes], val: BytesLike) -> "_HexBytes":
        return super().__new__(cls, val)
