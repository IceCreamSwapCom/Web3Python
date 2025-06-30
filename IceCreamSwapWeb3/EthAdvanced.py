import os
from time import sleep
from typing import Optional, TypedDict, Sequence

from eth_typing import BlockNumber, Address, ChecksumAddress
from eth_typing.evm import BlockParams
from hexbytes import HexBytes
from web3.eth import Eth
from web3.exceptions import ContractLogicError
from web3.types import FilterParams, LogReceipt, StateOverride, BlockIdentifier, TxParams, BlockData, _Hash32

from IceCreamSwapWeb3 import Web3Advanced
from IceCreamSwapWeb3.Subsquid import get_filter


class ForkedBlock(Exception):
    pass


class FilterParamsExtended(TypedDict, total=False):
    address: ChecksumAddress | list[ChecksumAddress]
    blockHash: HexBytes
    fromBlock: BlockParams | BlockNumber | int
    fromBlockParentHash: _Hash32
    toBlock: BlockParams | BlockNumber | int
    toBlockHash: _Hash32
    topics: Sequence[_Hash32 | Sequence[_Hash32] | None]


class FilterParamsSanitized(TypedDict, total=False):
    address: Address | ChecksumAddress | list[Address] | list[ChecksumAddress]
    blockHash: HexBytes
    fromBlock: int
    fromBlockParentHash: str
    toBlock: int
    toBlockHash: str
    topics: Sequence[_Hash32 | Sequence[_Hash32] | None]


def exponential_retry(func_name: str = None):
    def wrapper(func):
        def inner(*args, no_retry: bool = False, **kwargs):
            if no_retry:
                return func(*args, **kwargs)

            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if isinstance(e, ContractLogicError):
                        raise
                    if "unknown block" in str(e) and retries >= 3:
                        raise
                    if retries == 0:
                        wait_for = 0
                    elif retries < 6:
                        wait_for = 2 ** (retries - 1)
                    else:
                        wait_for = 30
                    print(f"Web3Advanced.eth.{func_name or func.__name__} threw \"{repr(e)}\" on {retries+1}th try, retrying in {wait_for}s")

                    retries += 1
                    sleep(wait_for)
        return inner
    return wrapper


RPC_TO_CHAIN_ID_CACHE: dict[str, int] = {}


class EthAdvanced(Eth):
    w3: Web3Advanced

    METHODS_TO_RETRY = [
        'fee_history', 'create_access_list', 'estimate_gas',
        'get_transaction', 'get_raw_transaction', 'get_raw_transaction_by_block',
        'send_transaction', 'send_raw_transaction', 'get_balance',
        'get_code', 'get_transaction_count', 'get_transaction_receipt',
        'wait_for_transaction_receipt', 'get_storage_at', 'replace_transaction',
        'modify_transaction', 'sign', 'sign_transaction', 'sign_typed_data', 'filter',
        'get_filter_changes', 'get_filter_logs', 'uninstall_filter'
    ]

    PROPERTIES_TO_RETRY = [
        'accounts', 'block_number', 'gas_price',
        'max_priority_fee', 'syncing'
    ]

    def __init__(self, w3):
        super().__init__(w3=w3)

        if self.w3.should_retry:
            self._wrap_methods_with_retry()

    def _wrap_methods_with_retry(self):
        for method_name in self.METHODS_TO_RETRY:
            method = getattr(self, method_name)
            setattr(self, method_name, exponential_retry(func_name=method_name)(method))

        for prop_name in self.PROPERTIES_TO_RETRY:
            prop = getattr(self.__class__, prop_name)
            wrapped_prop = property(exponential_retry(func_name=prop_name)(prop.fget))
            setattr(self.__class__, prop_name, wrapped_prop)

    def call(
            self,
            transaction: TxParams,
            block_identifier: Optional[BlockIdentifier] = None,
            state_override: Optional[StateOverride] = None,
            ccip_read_enabled: Optional[bool] = None,
            no_retry: bool = False,
    ):
        if "no_retry" in transaction:
            no_retry = transaction["no_retry"]
            del transaction["no_retry"]
        if not self.w3.should_retry:
            no_retry = True

        return exponential_retry(func_name="call")(super().call)(
            transaction=transaction,
            block_identifier=block_identifier,
            state_override=state_override,
            ccip_read_enabled=ccip_read_enabled,
            no_retry=no_retry,
        )

    def get_block_number(self, no_retry: bool = False, ignore_latest_seen_block: bool = False) -> BlockNumber:
        block_number: BlockNumber = exponential_retry(func_name="get_block_number")(super().get_block_number)(
            no_retry=no_retry or not self.w3.should_retry,
        )
        if not ignore_latest_seen_block and self.w3.latest_seen_block < block_number:
            self.w3.latest_seen_block = block_number
        return block_number

    def get_block(
            self,
            block_identifier: BlockIdentifier,
            full_transactions: bool = False,
            no_retry: bool = False
    ) -> BlockData:
        block: BlockData = exponential_retry(func_name="get_block")(super().get_block)(
            block_identifier=block_identifier,
            full_transactions=full_transactions,
            no_retry=no_retry or not self.w3.should_retry,
        )
        if self.w3.latest_seen_block < block["number"]:
            self.w3.latest_seen_block = block["number"]
        return block

    def get_logs(
            self,
            _filter_params: FilterParamsExtended,
            show_progress_bar: bool = False,
            p_bar=None,
            no_retry: bool = False,
            use_subsquid: bool = os.getenv("NO_SUBSQUID_LOGS") is None
    ) -> list[LogReceipt]:
        filter_block_range = self.w3.filter_block_range
        if filter_block_range == 0:
            raise Exception("RPC does not support eth_getLogs")

        # getting logs for a single block defined by its block hash. No drama
        if "blockHash" in _filter_params:
            assert "fromBlock" not in _filter_params and "toBlock" not in _filter_params
            return self.get_logs_inner(_filter_params, no_retry=no_retry)

        filter_params: FilterParamsSanitized = {**_filter_params}

        if "fromBlockParentHash" in filter_params:
            if filter_params["fromBlockParentHash"] is None:
                del filter_params["fromBlockParentHash"]
            elif not isinstance(filter_params["fromBlockParentHash"], str):
                filter_params["fromBlockParentHash"] = filter_params["fromBlockParentHash"].to_0x_hex()

        if "toBlockHash" in filter_params:
            if filter_params["toBlockHash"] is None:
                del filter_params["toBlockHash"]
            elif not isinstance(filter_params["toBlockHash"], str):
                filter_params["toBlockHash"] = filter_params["toBlockHash"].to_0x_hex()

        if "fromBlock" not in filter_params or not isinstance(filter_params["fromBlock"], int):
            assert "fromBlockParentHash" not in filter_params, "can not specify fromBlockParentHash without fromBlock number"
            filter_params["fromBlock"] = self.get_block(filter_params.get("fromBlock", "earliest"))["number"]

        if "toBlock" not in filter_params or not isinstance(filter_params["toBlock"], int):
            assert "toBlockHash" not in filter_params, "can not specify toBlockHash without toBlock number"
            filter_params["toBlock"] = self.get_block(filter_params.get("toBlock", "latest"))["number"]

        kwargs = dict(
            show_progress_bar=show_progress_bar,
            p_bar=p_bar,
            no_retry=no_retry,
            use_subsquid=use_subsquid,
        )

        from_block = filter_params["fromBlock"]
        to_block = filter_params["toBlock"]
        from_block_parent_hash = filter_params.get("fromBlockParentHash")
        to_block_hash = filter_params.get("toBlockHash")

        assert to_block >= from_block, f"from block after to block, {from_block=}, {to_block=}"

        # if logs for a single block are queried, and we know the block hash, query by it
        if from_block == to_block and to_block_hash is not None:
            single_hash_filter = {**filter_params, "blockHash": to_block_hash}
            del single_hash_filter["fromBlock"]
            del single_hash_filter["toBlock"]
            return self.get_logs_inner(single_hash_filter, no_retry=no_retry)

        # note: fromBlock and toBlock are both inclusive. e.g. 5 to 6 are 2 blocks
        num_blocks = to_block - from_block + 1

        # check if progress bar needs initialization
        if show_progress_bar and p_bar is None:
            # local import as tqdm is an optional dependency of this package
            from tqdm import tqdm
            p_bar = tqdm(total=num_blocks)
            kwargs["p_bar"] = p_bar

        if use_subsquid and self.w3.subsquid_available and from_block < self.w3.latest_seen_block - 1_000:
            kwargs["use_subsquid"] = False  # make sure we only try once with Subsquid
            try:
                # trying to get logs from SubSquid
                next_block, results = get_filter(
                    chain_id=self.chain_id,
                    filter_params=filter_params,
                    partial_allowed=True,
                    p_bar=p_bar,
                )
            except Exception as e:
                if not isinstance(e, ValueError) or "Subsquid only has indexed till block " not in str(e):
                    print(f"Getting logs from SubSquid threw exception {repr(e)}, falling back to RPC")
            else:
                assert next_block <= to_block + 1, "SubSquid returned logs for more blocks than specified"
                if next_block == to_block + 1:
                    return results
                return results + self.get_logs({**filter_params, "fromBlock": next_block}, **kwargs)

        # getting logs for a single block, which is not at the chain head. No drama
        if num_blocks == 1:
            return self.get_logs_inner(filter_params, no_retry=no_retry)

        # if we already know that the filter range is too large, split it
        if num_blocks > filter_block_range:
            results = []
            for filter_start in range(from_block, to_block + 1, filter_block_range):
                filter_end = min(filter_start + filter_block_range - 1, to_block)
                partial_filter = {
                    **filter_params,
                    "fromBlock": filter_start,
                    "toBlock": filter_end,
                }
                if to_block_hash is not None and filter_end != to_block:
                    del partial_filter["toBlockHash"]
                if from_block_parent_hash is not None and filter_start != from_block:
                    del partial_filter["fromBlockParentHash"]
                results += self.get_logs(partial_filter, **kwargs)
            return results

        # get logs and split on exception
        try:
            with self.w3.batch_requests() as batch:
                if from_block_parent_hash is not None:
                    batch.add(self._get_block(from_block))
                batch.add(self._get_logs(filter_params))
                batch.add(self._get_block(to_block))
            events: list[LogReceipt]
            to_block_body: BlockData
            batch_results = batch.execute()
            if from_block_parent_hash is not None:
                events, to_block_body = batch_results
            else:
                from_block_body: BlockData
                events, to_block_body, from_block_body = batch_results
                assert from_block_body["number"] == from_block, "eth_getLogs RPC returned unexpected from block number"
                if from_block_body["parentHash"].to_0x_hex() != from_block_parent_hash:
                    raise ForkedBlock(f"expected={from_block_parent_hash}, actual={from_block_body['parentHash'].to_0x_hex()}")

            assert to_block_body["number"] == to_block, "eth_getLogs RPC returned unexpected to block number"
            if to_block_hash is not None and to_block_body["hash"].to_0x_hex() != to_block_hash:
                raise ForkedBlock(f"expected={to_block_hash}, actual={to_block_body['hash'].to_0x_hex()}")

            if p_bar is not None:
                p_bar.update(num_blocks)
            return events
        except Exception as e:
            # split the filter range and try again
            print(f"eth_getLogs between block {from_block} and {to_block} returned {repr(e)}, splitting and retrying")
            mid_block = (from_block + to_block) // 2
            left_filter = {**filter_params, "toBlock": mid_block}
            right_filter = {**filter_params, "fromBlock": mid_block + 1}
            if "toBlockHash" in left_filter:
                del left_filter["toBlockHash"]
            if "fromBlockParentHash" in right_filter:
                del right_filter["fromBlockParentHash"]
            return self.get_logs(left_filter, **kwargs) + self.get_logs(right_filter, **kwargs)

    def get_logs_inner(self, filter_params: FilterParams, no_retry: bool = False):
        if not self.w3.should_retry:
            no_retry = True
        return exponential_retry(func_name="get_logs")(self._get_logs)(filter_params, no_retry=no_retry)

    def _chain_id(self):
        rpc = self.w3.node_url
        if rpc not in RPC_TO_CHAIN_ID_CACHE:
            RPC_TO_CHAIN_ID_CACHE[rpc] = exponential_retry(func_name="chain_id")(
                super()._chain_id
            )(no_retry=not self.w3.should_retry)
        return RPC_TO_CHAIN_ID_CACHE[rpc]


def main(
        node_url="https://rpc-core.icecreamswap.com",
        usdt_address="0x900101d06A7426441Ae63e9AB3B9b0F63Be145F1",
):
    from .FastChecksumAddress import to_checksum_address

    usdt_address = to_checksum_address(usdt_address)

    w3 = Web3Advanced(node_url=node_url)

    latest_block = w3.eth.block_number
    logs = w3.eth.get_logs({
        "address": usdt_address,
        "fromBlock": latest_block - 10_000,
        "toBlock": latest_block,
    }, use_subsquid=False, get_logs_by_block_hash=True)
    print(len(logs), logs[0])

if __name__ == "__main__":
    main()
