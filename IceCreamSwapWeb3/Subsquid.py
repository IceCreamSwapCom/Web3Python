import requests
from eth_utils import to_checksum_address
from hexbytes import HexBytes
from tqdm import tqdm
from web3.types import FilterParams, LogReceipt

ENDPOINTS = {
    16600: 'https://v2.archive.subsquid.io/network/0g-testnet',
    41455: 'https://v2.archive.subsquid.io/network/aleph-zero-evm-mainnet',
    42161: 'https://v2.archive.subsquid.io/network/arbitrum-one',
    42170: 'https://v2.archive.subsquid.io/network/arbitrum-nova',
    421614: 'https://v2.archive.subsquid.io/network/arbitrum-sepolia',
    10242: 'https://v2.archive.subsquid.io/network/arthera-mainnet',
    592: 'https://v2.archive.subsquid.io/network/astar-mainnet',
    3776: 'https://v2.archive.subsquid.io/network/astar-zkevm-mainnet',
    6038361: 'https://v2.archive.subsquid.io/network/astar-zkyoto',
    43114: 'https://v2.archive.subsquid.io/network/avalanche-mainnet',
    43113: 'https://v2.archive.subsquid.io/network/avalanche-testnet',
    8333: 'https://v2.archive.subsquid.io/network/b3-mainnet',
    1993: 'https://v2.archive.subsquid.io/network/b3-sepolia',
    8453: 'https://v2.archive.subsquid.io/network/base-mainnet',
    84532: 'https://v2.archive.subsquid.io/network/base-sepolia',
    80084: 'https://v2.archive.subsquid.io/network/berachain-bartio',
    56: 'https://v2.archive.subsquid.io/network/binance-mainnet',
    97: 'https://v2.archive.subsquid.io/network/binance-testnet',
    355110: 'https://v2.archive.subsquid.io/network/bitfinity-mainnet',
    355113: 'https://v2.archive.subsquid.io/network/bitfinity-testnet',
    32520: 'https://v2.archive.subsquid.io/network/bitgert-mainnet',
    64668: 'https://v2.archive.subsquid.io/network/bitgert-testnet',
    81457: 'https://v2.archive.subsquid.io/network/blast-l2-mainnet',
    168587773: 'https://v2.archive.subsquid.io/network/blast-sepolia',
    60808: 'https://v2.archive.subsquid.io/network/bob-mainnet',
    7700: 'https://v2.archive.subsquid.io/network/canto',
    7701: 'https://v2.archive.subsquid.io/network/canto-testnet',
    44787: 'https://v2.archive.subsquid.io/network/celo-alfajores-testnet',
    42220: 'https://v2.archive.subsquid.io/network/celo-mainnet',
    1116: 'https://v2.archive.subsquid.io/network/core-mainnet',
    4157: 'https://v2.archive.subsquid.io/network/crossfi-testnet',
    7560: 'https://v2.archive.subsquid.io/network/cyber-mainnet',
    111557560: 'https://v2.archive.subsquid.io/network/cyberconnect-l2-testnet',
    666666666: 'https://v2.archive.subsquid.io/network/degen-chain',
    53935: 'https://v2.archive.subsquid.io/network/dfk-chain',
    2000: 'https://v2.archive.subsquid.io/network/dogechain-mainnet',
    568: 'https://v2.archive.subsquid.io/network/dogechain-testnet',
    17000: 'https://v2.archive.subsquid.io/network/ethereum-holesky',
    1: 'https://v2.archive.subsquid.io/network/ethereum-mainnet',
    11155111: 'https://v2.archive.subsquid.io/network/ethereum-sepolia',
    42793: 'https://v2.archive.subsquid.io/network/etherlink-mainnet',
    128123: 'https://v2.archive.subsquid.io/network/etherlink-testnet',
    2109: 'https://v2.archive.subsquid.io/network/exosama',
    250: 'https://v2.archive.subsquid.io/network/fantom-mainnet',
    4002: 'https://v2.archive.subsquid.io/network/fantom-testnet',
    14: 'https://v2.archive.subsquid.io/network/flare-mainnet',
    1625: 'https://v2.archive.subsquid.io/network/galxe-gravity',
    88153591557: 'https://v2.archive.subsquid.io/network/gelato-arbitrum-blueberry',
    123420111: 'https://v2.archive.subsquid.io/network/gelato-opcelestia-raspberry',
    100: 'https://v2.archive.subsquid.io/network/gnosis-mainnet',
    13371: 'https://v2.archive.subsquid.io/network/immutable-zkevm-mainnet',
    13473: 'https://v2.archive.subsquid.io/network/immutable-zkevm-testnet',
    1998: 'https://v2.archive.subsquid.io/network/kyoto-testnet',
    59144: 'https://v2.archive.subsquid.io/network/linea-mainnet',
    169: 'https://v2.archive.subsquid.io/network/manta-pacific',
    3441006: 'https://v2.archive.subsquid.io/network/manta-pacific-sepolia',
    5000: 'https://v2.archive.subsquid.io/network/mantle-mainnet',
    5003: 'https://v2.archive.subsquid.io/network/mantle-sepolia',
    4200: 'https://v2.archive.subsquid.io/network/merlin-mainnet',
    686868: 'https://v2.archive.subsquid.io/network/merlin-testnet',
    1088: 'https://v2.archive.subsquid.io/network/metis-mainnet',
    34443: 'https://v2.archive.subsquid.io/network/mode-mainnet',
    1287: 'https://v2.archive.subsquid.io/network/moonbase-testnet',
    1284: 'https://v2.archive.subsquid.io/network/moonbeam-mainnet',
    1285: 'https://v2.archive.subsquid.io/network/moonriver-mainnet',
    42225: 'https://v2.archive.subsquid.io/network/nakachain',
    245022934: 'https://v2.archive.subsquid.io/network/neon-devnet',
    245022926: 'https://v2.archive.subsquid.io/network/neon-mainnet',
    204: 'https://v2.archive.subsquid.io/network/opbnb-mainnet',
    5611: 'https://v2.archive.subsquid.io/network/opbnb-testnet',
    10: 'https://v2.archive.subsquid.io/network/optimism-mainnet',
    11155420: 'https://v2.archive.subsquid.io/network/optimism-sepolia',
    3338: 'https://v2.archive.subsquid.io/network/peaq-mainnet',
    98864: 'https://v2.archive.subsquid.io/network/plume-testnet',
    137: 'https://v2.archive.subsquid.io/network/polygon-mainnet',
    80002: 'https://v2.archive.subsquid.io/network/polygon-amoy-testnet',
    1101: 'https://v2.archive.subsquid.io/network/polygon-zkevm-mainnet',
    2442: 'https://v2.archive.subsquid.io/network/polygon-zkevm-cardona-testnet',
    97072271: 'https://v2.archive.subsquid.io/network/prom-testnet',
    584548796: 'https://v2.archive.subsquid.io/network/prom-testnet-v2',
    157: 'https://v2.archive.subsquid.io/network/puppynet',
    534352: 'https://v2.archive.subsquid.io/network/scroll-mainnet',
    534351: 'https://v2.archive.subsquid.io/network/scroll-sepolia',
    109: 'https://v2.archive.subsquid.io/network/shibarium',
    81: 'https://v2.archive.subsquid.io/network/shibuya-testnet',
    336: 'https://v2.archive.subsquid.io/network/shiden-mainnet',
    1482601649: 'https://v2.archive.subsquid.io/network/skale-nebula',
    64165: 'https://v2.archive.subsquid.io/network/sonic-testnet',
    93747: 'https://v2.archive.subsquid.io/network/stratovm-sepolia',
    53302: 'https://v2.archive.subsquid.io/network/superseed-sepolia',
    167000: 'https://v2.archive.subsquid.io/network/taiko-mainnet',
    5678: 'https://v2.archive.subsquid.io/network/tanssi',
    196: 'https://v2.archive.subsquid.io/network/xlayer-mainnet',
    195: 'https://v2.archive.subsquid.io/network/xlayer-testnet',
    810180: 'https://v2.archive.subsquid.io/network/zklink-nova-mainnet',
    324: 'https://v2.archive.subsquid.io/network/zksync-mainnet',
    300: 'https://v2.archive.subsquid.io/network/zksync-sepolia',
    7777777: 'https://v2.archive.subsquid.io/network/zora-mainnet',
    999999999: 'https://v2.archive.subsquid.io/network/zora-sepolia',
}

def get_text(url: str) -> str:
    res = requests.get(url)
    res.raise_for_status()
    return res.text

def get_filter(
        chain_id: int,
        filter_params: FilterParams,
        partial_allowed=False,
        p_bar: tqdm = None
) -> tuple[int, list[LogReceipt]]:
    if chain_id not in ENDPOINTS:
        raise ValueError(f"Subsquid does not support Chain ID {chain_id}")

    gateway_url = ENDPOINTS[chain_id]

    assert isinstance(filter_params['fromBlock'], int)
    assert isinstance(filter_params['toBlock'], int)
    from_block: int = filter_params['fromBlock']
    to_block: int = filter_params['toBlock']

    latest_block = int(get_text(f"{gateway_url}/height"))

    if from_block > latest_block:
        raise ValueError(f"Subsquid has only indexed till block {latest_block}")

    if to_block > latest_block:
        if partial_allowed:
            to_block = latest_block
        else:
            raise ValueError(f"Subsquid has only indexed till block {latest_block}")

    query = {
        "toBlock": to_block,
        "logs": [{}],
        "fields": {
            "log": {
                "address": True,
                "topics": True,
                "data": True,
                "transactionHash": True,
            }
        }
    }
    if "address" in filter_params:
        addresses = filter_params['address']
        if isinstance(addresses, str):
            addresses = [addresses]
        query["logs"][0]["address"] = [address.lower() for address in addresses]
    if "topics" in filter_params:
        topics = filter_params["topics"]
        assert len(topics) <= 4
        for i in range(len(topics)):
            topic: str | list[str]
            if isinstance(topics[i], str):
                topic = [topics[i]]
            elif hasattr(topics[i], "hex"):
                topic = [topics[i].hex()]
            else:
                topic = [(single_topic.hex() if not isinstance(single_topic, str) else single_topic) for single_topic in topics[i]]
            query["logs"][0][f"topic{i}"] = topic

    logs: list[LogReceipt] = []
    while from_block <= to_block:
        worker_url = get_text(f'{gateway_url}/{from_block}/worker')

        query['fromBlock'] = from_block
        res = requests.post(worker_url, json=query)
        res.raise_for_status()
        blocks = res.json()

        last_processed_block = blocks[-1]['header']['number']
        if p_bar is not None:
            p_bar.update(last_processed_block-from_block+1)
        from_block = last_processed_block + 1

        for block in blocks:
            for log in block['logs']:
                logs.append(LogReceipt(
                    address=to_checksum_address(log['address']),
                    blockHash=block["header"]["hash"],
                    blockNumber=block["header"]["number"],
                    data=HexBytes(log["data"]),
                    logIndex=log["logIndex"],
                    topics=[HexBytes(topic) for topic in log["topics"]],
                    transactionHash=HexBytes(log["transactionHash"]),
                    transactionIndex=log["transactionIndex"],
                    removed=False,
                ))
    return from_block, logs
