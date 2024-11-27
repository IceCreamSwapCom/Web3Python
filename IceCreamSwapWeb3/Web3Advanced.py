import os
from importlib.resources import files

from eth_utils import to_checksum_address
from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.main import get_default_modules
from web3.middleware import ExtraDataToPOAMiddleware

from .EthAdvanced import EthAdvanced
from .Multicall import MultiCall
from .Web3ErrorHandlerPatch import patch_error_formatters


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

    def __init__(
            self,
            node_url: str,
            should_retry: bool = True,
            unstable_blocks: int = int(os.getenv("UNSTABLE_BLOCKS", 5)),  # not all nodes might have latest n blocks, these are seen as unstable
    ):
        patch_error_formatters()
        self.node_url = node_url
        self.should_retry = should_retry
        self.unstable_blocks = unstable_blocks

        provider = self._construct_provider(node_url=self.node_url)

        # use the EthAdvanced class instead of the Eth class for w3.eth
        modules = get_default_modules()
        modules["eth"] = EthAdvanced

        super().__init__(provider=provider, modules=modules)

        self.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0, name="poa")  # required for pos chains

        self.latest_seen_block = self.eth.get_block_number(ignore_latest_seen_block=True)

        self.filter_block_range = self._find_max_filter_range()
        self.revert_reason_available: bool = self._check_revert_reason_available()
        if not self.revert_reason_available:
            print(f"RPC {self.node_url} does not return revert reasons")

    @staticmethod
    def _construct_provider(node_url):
        assert "://" in node_url
        protocol = node_url.split("://")[0]
        if protocol in ("https", "http"):
            return Web3.HTTPProvider(node_url)
        elif protocol in ("ws", "wss"):
            return Web3.WebsocketProvider(node_url)
        else:
            raise ValueError(f"Unknown protocol for RPC URL {node_url}")

    def start_multicall(self) -> MultiCall:
        return MultiCall(w3=self)

    def _find_max_filter_range(self):
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
            except Exception:
                pass
        print(f"Can not use eth_getLogs with RPC {self.node_url}")
        return 0

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
            return False
        except Exception as e:
            if not isinstance(e, ContractLogicError):
                return False
            return e.message == "execution reverted: abc"
