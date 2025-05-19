import os
from importlib.resources import files
from time import sleep

from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.main import get_default_modules
from web3.middleware import ExtraDataToPOAMiddleware

from .BatchRetryMiddleware import BatchRetryMiddleware
from .EthAdvanced import EthAdvanced
from .Multicall import MultiCall
from .Subsquid import get_endpoints
from .Web3ErrorHandlerPatch import patch_error_formatters
from .FastChecksumAddress import to_checksum_address

patch_error_formatters()


class Web3Advanced(Web3):
    eth: EthAdvanced

    FILTER_RANGES_TO_TRY = sorted([
        10_000,
        5_000,
        2_000,
        1_000,
        500,
        200,
        100,
        50,
        20,
        10,
        5,
        2,
        1
    ], reverse=True)
    assert FILTER_RANGES_TO_TRY[-1] == 1

    BATCH_SIZES_TO_TRY = sorted([
        1_000,
        500,
        200,
        100,
        50,
        20,
        10,
        5,
        2,
        1
    ], reverse=True)
    assert BATCH_SIZES_TO_TRY[-1] == 1

    def __init__(
            self,
            node_url: str,
            should_retry: bool = True,
            unstable_blocks: int = int(os.getenv("UNSTABLE_BLOCKS", 5)),  # not all nodes might have latest n blocks, these are seen as unstable
    ):
        self.node_url = node_url
        self.should_retry = should_retry
        self.unstable_blocks = unstable_blocks

        super().__init__(provider=self._construct_provider(node_url=self.node_url), modules=self._get_modules())

        self.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0, name="poa")  # required for pos chains

        self.latest_seen_block = self.eth.get_block_number(ignore_latest_seen_block=True)

        self.filter_block_range = self._find_max_filter_range()
        self.rpc_batch_max_size = self._find_max_batch_size()
        self.revert_reason_available: bool = self._check_revert_reason_available()
        self.is_archive = self._check_is_archive()
        self.overwrites_available: bool = self._check_overwrites_available()
        self.subsquid_available: bool = self._check_subsquid_available()

        self.middleware_onion.inject(BatchRetryMiddleware, layer=0, name="batch_retry")  # split and retry batch requests

    def __deepcopy__(self, memo):
        # create new instance, but only call init of Web3.py, not our custom one.
        new = self.__class__.__new__(self.__class__)
        memo[id(self)] = new

        # Copy over all our custom data instead of running the lengthy checks of our init again
        new.node_url = self.node_url
        new.should_retry = self.should_retry
        new.unstable_blocks = self.unstable_blocks
        new.latest_seen_block = self.latest_seen_block
        new.filter_block_range = self.filter_block_range
        new.rpc_batch_max_size = self.rpc_batch_max_size
        new.revert_reason_available = self.revert_reason_available
        new.is_archive = self.is_archive
        new.overwrites_available = self.overwrites_available
        new.subsquid_available = self.subsquid_available

        # only call Web3.py init, not init of Web3Advanced
        Web3.__init__(
            new,
            provider=self._construct_provider(node_url=self.node_url),
            modules=self._get_modules()
        )

        # initialize custom middlewares
        new.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0, name="poa")
        new.middleware_onion.inject(BatchRetryMiddleware, layer=0, name="batch_retry")

        return new

    @staticmethod
    def _get_modules():
        # use the EthAdvanced class instead of the Eth class for w3.eth
        modules = get_default_modules()
        modules["eth"] = EthAdvanced
        return modules

    @staticmethod
    def _construct_provider(node_url):
        assert "://" in node_url
        protocol = node_url.split("://")[0]
        if protocol in ("https", "http"):
            return Web3.HTTPProvider(node_url, exception_retry_configuration=None)
        elif protocol in ("ws", "wss"):
            return Web3.WebsocketProvider(node_url)
        else:
            raise ValueError(f"Unknown protocol for RPC URL {node_url}")

    def start_multicall(self) -> MultiCall:
        return MultiCall(w3=self)

    def _find_max_filter_range(self) -> int:
        current_block = self.eth.block_number
        for filter_range in self.FILTER_RANGES_TO_TRY:
            try:
                # getting logs from the 0 address as it does not emit any logs.
                # This way we can test the maximum allowed filter range without getting back a ton of logs
                result = self.eth._get_logs({
                    "address": to_checksum_address("0x0000000000000000000000000000000000000000"),
                    "fromBlock": current_block - 5 - filter_range + 1,
                    "toBlock": current_block - 5,
                })
                assert result == []
                return filter_range
            except Exception as e:
                if filter_range == self.FILTER_RANGES_TO_TRY[-1]:
                    print(f"Can not use eth_getLogs, got: {repr(e)}")
                else:
                    sleep(0.1)
        return 0

    def _find_max_batch_size(self) -> int:
        working_size = 0
        try:
            for batch_size in reversed(self.BATCH_SIZES_TO_TRY):
                with self.batch_requests() as batch:
                    for _ in range(batch_size):
                        batch.add(self.eth._gas_price())
                    result = batch.execute()
                assert len(result) == batch_size
                working_size = batch_size
                sleep(0.1)
        except Exception as e:
            if working_size == 0:
                print(f"RPC does not support batch requests, got: {repr(e)}")

        return working_size

    def _check_is_archive(self):
        try:
            self.eth.call({
                "to": "0x0000000000000000000000000000000000000000",
                "data": f"{0:064x}"
            }, block_identifier=1, no_retry=True)
            return True
        except Exception as e:
            print(f"RPC does not support archive requests, got: {repr(e)}")
            return False

    def _check_revert_reason_available(self):
        with files("IceCreamSwapWeb3").joinpath("./abi/RevertTester.abi").open('r') as f:
            revert_tester_abi = f.read()
        with files("IceCreamSwapWeb3").joinpath("./bytecode/RevertTester.bytecode").open('r') as f:
            revert_tester_bytecode = f.read()
        revert_tester_contract = self.eth.contract(abi=revert_tester_abi, bytecode=revert_tester_bytecode)
        try:
            self.eth.call({
                "data": revert_tester_contract.constructor().data_in_transaction
            }, no_retry=True)
            # should revert, if not, reverts are useless
            print(f"RPC does not revert besides it should")
            return False
        except Exception as e:
            if not isinstance(e, ContractLogicError):
                print(f"RPC does not properly return revert reasons, got: {repr(e)}")
                return False
            available = e.message == "execution reverted: abc"
            if not available:
                print(f"RPC does not return expected revert reasons, got: {repr(e)}")
            return available

    def _check_overwrites_available(self) -> bool:
        with files("IceCreamSwapWeb3").joinpath("./abi/OverwriteTester.abi").open('r') as f:
            overwrite_tester_abi = f.read()
        with files("IceCreamSwapWeb3").joinpath("./bytecode/OverwriteTesterRuntime.bytecode").open('r') as f:
            overwrite_tester_bytecode = f.read()

        test_address = to_checksum_address("0x1234567800000000000000000000000000000001")
        test_value = 1234
        overwrite_tester_contract = self.eth.contract(abi=overwrite_tester_abi, address=test_address)
        try:
            response = overwrite_tester_contract.functions.getSlot0().call(
                transaction={"no_retry": True},
                state_override={
                    test_address: {
                        "code": overwrite_tester_bytecode,
                        "stateDiff": {
                            "0x" + "00" * 32: "0x" + hex(test_value)[2:].rjust(64, "0")
                        }
                    }
                }
            )
        except Exception as e:
            print(f"RPC does not support state overwrites, got: {repr(e)}")
            return False
        return response == test_value

    def _check_subsquid_available(self) -> bool:
        try:
            endpoints = get_endpoints()
        except Exception as e:
            print(f"Could not get supported chains from SubSquid: {repr(e)}")
            return False

        chain_id = self.eth.chain_id
        if chain_id not in endpoints:
            print(f"SubSquid does not support chain {chain_id}")
            return False

        return True
