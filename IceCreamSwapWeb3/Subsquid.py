from typing import cast

import os
import json
import requests
from .FastChecksumAddress import to_checksum_address
from hexbytes import HexBytes
from web3.types import FilterParams, LogReceipt


endpoint_cache: dict[int, str] | None = None
def get_endpoints() -> dict[int, str]:
    global endpoint_cache
    if endpoint_cache is None:
        res = requests.get("https://cdn.subsquid.io/archives/evm.json")
        res.raise_for_status()

        endpoints: dict[int, str] = {}
        for chain in res.json()["archives"]:
            endpoints[chain["chainId"]] = chain["providers"][0]["dataSourceUrl"]

        endpoint_cache = endpoints
    return endpoint_cache

latest_block_cache: int | None = None
def get_latest_subsquid_block(gateway_url: str) -> int:
    global latest_block_cache
    if latest_block_cache is None:
        latest_block_cache = int(get_text(f"{gateway_url}/height"))

    return latest_block_cache

def get_text(url: str) -> str:
    res = requests.get(url)
    res.raise_for_status()
    return res.text

# getting up to the next 100k blocks in anticipation of future queries.
future_logs_cache = {}
def get_filter(
        chain_id: int,
        filter_params: FilterParams,
        partial_allowed=False,
        disable_subsquid_look_ahead_cache: bool = os.getenv("DISABLE_SUBSQUID_LOOKAHEAD_CACHE", "false").lower() == "true",
        p_bar = None
) -> tuple[int, list[LogReceipt]]:
    endpoints = get_endpoints()
    if chain_id not in endpoints:
        raise ValueError(f"Subsquid does not support Chain ID {chain_id}")

    gateway_url = endpoints[chain_id]

    assert isinstance(filter_params['fromBlock'], int)
    assert isinstance(filter_params['toBlock'], int)
    from_block: int = filter_params['fromBlock']
    to_block: int = filter_params['toBlock']

    latest_block = get_latest_subsquid_block(gateway_url)

    if from_block > latest_block:
        raise ValueError(f"Subsquid only has indexed till block {latest_block}")

    if to_block > latest_block:
        if partial_allowed:
            to_block = latest_block
        else:
            raise ValueError(f"Subsquid has only indexed till block {latest_block}")

    query = {
        "toBlock": to_block if disable_subsquid_look_ahead_cache else to_block + 100_000,
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
            topic: list[str]
            if topics[i] is None:
                continue
            elif isinstance(topics[i], str):
                topic = [topics[i]]
            elif hasattr(topics[i], "to_0x_hex"):
                topic = [topics[i].to_0x_hex()]
            else:
                topic = [(single_topic.to_0x_hex() if not isinstance(single_topic, str) else single_topic) for single_topic in topics[i]]
            query["logs"][0][f"topic{i}"] = topic

    logs: list[LogReceipt] = []
    while from_block <= to_block:
        cache_key_query = query.copy()
        cache_key_query.pop("fromBlock", None)
        cache_key_query.pop("toBlock")
        cache_key = json.dumps(cache_key_query)

        if cache_key in future_logs_cache and future_logs_cache[cache_key]["fromBlock"] == from_block:
            blocks = future_logs_cache.pop(cache_key)["blocks"]
        else:
            worker_url = get_text(f'{gateway_url}/{from_block}/worker')

            if os.getenv("SUBSQUID_USE_IP_PROXY", "true").lower() == "true":
                assert worker_url.startswith("https://")
                worker_url = "https://rpc-internal.icecreamswap.com/proxy/" + worker_url[8:]

            query['fromBlock'] = from_block
            res = requests.post(worker_url, json=query)
            res.raise_for_status()
            blocks = res.json()

        # got more results than needed right now. Caching additional results
        if blocks[-1]['header']['number'] > to_block:
            if not disable_subsquid_look_ahead_cache:
                if len(future_logs_cache) > 10:
                    # limiting future_logs_cache to 10 items
                    future_logs_cache.pop(next(iter(future_logs_cache)))
                future_blocks = [block for block in blocks if block['header']['number'] > to_block]
                future_logs_cache[cache_key] = {
                    "fromBlock": to_block + 1,
                    "blocks": future_blocks,
                }
            blocks = [block for block in blocks if block['header']['number'] <= to_block]
            last_processed_block = to_block
        else:
            last_processed_block = blocks[-1]['header']['number']

        assert last_processed_block <= to_block
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
