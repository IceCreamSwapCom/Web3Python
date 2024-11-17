from time import sleep
from typing import Optional

from eth_typing import BlockNumber
from web3.eth import Eth
from web3.exceptions import ContractLogicError
from web3.types import FilterParams, LogReceipt, CallOverride, BlockIdentifier, TxParams, BlockData

from IceCreamSwapWeb3 import Web3Advanced
from IceCreamSwapWeb3.Subsquid import get_filter


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


class EthAdvanced(Eth):
    w3: Web3Advanced

    METHODS_TO_RETRY = [
        'fee_history', 'create_access_list', 'estimate_gas',
        'get_transaction', 'get_raw_transaction', 'get_raw_transaction_by_block',
        'send_transaction', 'send_raw_transaction', 'get_balance',
        'get_code', 'get_transaction_count', 'get_transaction_receipt',
        'wait_for_transaction_receipt', 'get_storage_at', 'replace_transaction',
        'modify_transaction', 'sign', 'sign_transaction', 'sign_typed_data', 'filter',
        'get_filter_changes', 'get_filter_logs', 'uninstall_filter', 'submit_hashrate',
        'get_work', 'submit_work',
    ]

    PROPERTIES_TO_RETRY = [
        'accounts', 'hashrate', 'block_number', 'coinbase', 'gas_price',
        'max_priority_fee', 'mining', 'syncing'
    ]

    def __init__(self, w3):
        super().__init__(w3=w3)

        if self.w3.should_retry:
            self._wrap_methods_with_retry()

        self.chain_id_cached = super()._chain_id()

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
            state_override: Optional[CallOverride] = None,
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
            filter_params: FilterParams,
            show_progress_bar: bool = False,
            p_bar=None,
            no_retry: bool = False,
            use_subsquid: bool = True,
    ) -> list[LogReceipt]:
        filter_block_range = self.w3.filter_block_range
        if filter_block_range == 0:
            raise Exception("RPC does not support eth_getLogs")

        # getting logs for a single block defined by its block hash. No drama
        if "blockHash" in filter_params:
            assert "fromBlock" not in filter_params and "toBlock" not in filter_params
            return self.get_logs_inner(filter_params, no_retry=no_retry)

        from_block_original: BlockIdentifier = filter_params.get("fromBlock", "earliest")
        to_block_original: BlockIdentifier = filter_params.get("toBlock", "latest")

        # sanitizing block numbers, could be strings like "latest"
        if isinstance(from_block_original, int):
            from_block_body = None
            from_block = from_block_original
        else:
            from_block_body = self.get_block(from_block_original)
            from_block = from_block_body["number"]
            filter_params = {**filter_params, "fromBlock": from_block}

        if isinstance(to_block_original, int):
            to_block_body = None
            to_block = to_block_original
        else:
            to_block_body = self.get_block(to_block_original)
            to_block = to_block_body["number"]
            filter_params = {**filter_params, "toBlock": to_block}

        assert to_block >= from_block, f"{from_block=}, {to_block=}"

        # if logs for a single block are queried, and we know the block hash, query by it
        if from_block == to_block and (from_block_body or to_block_body):
            block_body = from_block_body if from_block_body else to_block_body
            single_hash_filter = {**filter_params, "blockHash": block_body["hash"]}
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

        kwargs = dict(
            show_progress_bar=show_progress_bar,
            p_bar=p_bar,
            no_retry=no_retry,
            use_subsquid=use_subsquid,
        )

        # the latest blocks might be available on some nodes but not at others.
        # setting toBlock to a block bigger than the latest known block of the node
        # simply ignores logs from the missing block
        # to prevent this, we get the latest blocks individually by their hashes
        unstable_blocks = self.w3.unstable_blocks
        if to_block > self.w3.latest_seen_block - unstable_blocks and to_block > (last_stable_block := (self.get_block_number() - unstable_blocks)):
            results = []
            if from_block <= last_stable_block:
                results += self.get_logs({**filter_params, "toBlock": last_stable_block}, **kwargs)

            # get all block hashes and ensure they build upon each other
            block_hashes = []
            for block_number in range(max(last_stable_block + 1, from_block), to_block + 1):
                block = self.get_block(block_number, no_retry=no_retry)
                if block_hashes:
                    # make sure chain of blocks is consistent with each block building on the previous one
                    assert block["parentHash"] == block_hashes[-1], f"{block_hashes[-1]=}, {block['parentHash']=}"
                if block_number == from_block and from_block_body is not None:
                    assert block["hash"] == from_block_body["hash"], f"{from_block_body['hash']=}, {block['hash']=}"
                if block_number == to_block and to_block_body is not None:
                    assert block["hash"] == to_block_body["hash"], f"{to_block_body['hash']=}, {block['hash']=}"
                block_hashes.append(block["hash"])

            single_hash_filter = filter_params.copy()
            del single_hash_filter["fromBlock"]
            del single_hash_filter["toBlock"]
            for block_hash in block_hashes:
                results += self.get_logs_inner({**single_hash_filter, "blockHash": block_hash}, no_retry=no_retry)
                if p_bar is not None:
                    p_bar.update(1)
            return results

        # getting logs for a single block, which is not at the chain head. No drama
        if num_blocks == 1:
            return self.get_logs_inner(filter_params, no_retry=no_retry)

        if use_subsquid and from_block < self.w3.latest_seen_block - 100:
            kwargs["use_subsquid"] = False  # make sure we only try once with Subsquid
            try:
                # trying to get logs from SubSquid
                till_block, results = get_filter(
                    chain_id=self.chain_id,
                    filter_params=filter_params,
                    partial_allowed=True,
                    p_bar=p_bar,
                )
                if till_block >= to_block:
                    return results
                return results + self.get_logs({**filter_params, "fromBlock": till_block + 1}, **kwargs)
            except Exception as e:
                print(f"Getting logs from SubSquid threw exception {repr(e)}, falling back to RPC")

        # if we already know that the filter range is too large, split it
        if num_blocks > filter_block_range:
            results = []
            for filter_start in range(from_block, to_block + 1, filter_block_range):
                results += self.get_logs({
                    **filter_params,
                    "fromBlock": filter_start,
                    "toBlock": min(filter_start + filter_block_range - 1, to_block),
                }, **kwargs)
            return results

        # get logs
        try:
            events = self._get_logs(filter_params)
            if p_bar is not None:
                p_bar.update(num_blocks)
            return events
        except Exception:
            # split the filter range and try again
            mid_block = (from_block + to_block) // 2
            left_filter = {**filter_params, "toBlock": mid_block}
            right_filter = {**filter_params, "fromBlock": mid_block + 1}
            return self.get_logs(left_filter, **kwargs) + self.get_logs(right_filter, **kwargs)

    def get_logs_inner(self, filter_params: FilterParams, no_retry: bool = False):
        if not self.w3.should_retry:
            no_retry = True
        return exponential_retry(func_name="get_logs")(self._get_logs)(filter_params, no_retry=no_retry)

    def get_block_number_from_identifier(self, block_identifier: BlockIdentifier) -> BlockNumber:
        return block_identifier if isinstance(block_identifier, int) else self.get_block(block_identifier)["number"]

    def _chain_id(self):
        # usually this causes an RPC call and is used in every eth_call. Getting it once in the init and then not again.
        return self.chain_id_cached
