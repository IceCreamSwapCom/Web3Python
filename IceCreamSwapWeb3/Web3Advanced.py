from web3 import Web3, JSONBaseProvider
from web3.main import get_default_modules
from web3.middleware import geth_poa_middleware

from EthAdvanced import EthAdvanced


class Web3Advanced(Web3):
    eth: EthAdvanced

    def __init__(
            self,
            node_url: str,
    ):
        provider = self._construct_provider(node_url=node_url)

        # use the EthAdvanced class instead of the Eth class for w3.eth
        modules = get_default_modules()
        modules["eth"] = EthAdvanced

        super().__init__(provider=provider, modules=modules)

        self.middleware_onion.inject(geth_poa_middleware, layer=0, name="poa")  # required for pos chains
        self._chain_id = self.eth.chain_id  # avoids many RPC calls to get chain ID

    @staticmethod
    def _construct_provider(node_url) -> JSONBaseProvider:
        assert "://" in node_url
        protocol = node_url.split("://")[0]
        if protocol in ("https", "http"):
            return Web3.HTTPProvider(node_url)
        elif protocol in ("ws", "wss"):
            return Web3.WebsocketProvider(node_url)
        else:
            raise ValueError(f"Unknown protocol for RPC URL {node_url}")
