from typing import cast

import requests
from .FastChecksumAddress import to_checksum_address
from hexbytes import HexBytes
from tqdm import tqdm
from web3.types import FilterParams, LogReceipt


endpoint_cache: dict[int, str] | None = None
def get_endpoints() -> dict[int, str]:
    global endpoint_cache
    if endpoint_cache is not None:
        return endpoint_cache
    res = requests.get("https://cdn.subsquid.io/archives/evm.json")
    res.raise_for_status()

    endpoints: dict[int, str] = {}
    for chain in res.json()["archives"]:
        endpoints[chain["chainId"]] = chain["providers"][0]["dataSourceUrl"]

    endpoint_cache = endpoints
    return endpoints

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
    endpoints = get_endpoints()
    if chain_id not in endpoints:
        raise ValueError(f"Subsquid does not support Chain ID {chain_id}")

    gateway_url = endpoints[chain_id]

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
                    data=cast(HexBytes, bytes.fromhex(log["data"][2:])),
                    logIndex=log["logIndex"],
                    topics=[cast(HexBytes, bytes.fromhex(topic[2:])) for topic in log["topics"]],
                    transactionHash=cast(HexBytes, bytes.fromhex(log["transactionHash"][2:])),
                    transactionIndex=log["transactionIndex"],
                    removed=False,
                ))
    return from_block, logs
