from time import sleep

from web3.manager import NULL_RESPONSES
from web3.middleware import Web3Middleware

from IceCreamSwapWeb3 import Web3Advanced
from IceCreamSwapWeb3.EthAdvanced import exponential_retry


class BatchRetryMiddleware(Web3Middleware):
    _w3: Web3Advanced

    def wrap_make_batch_request(self, make_batch_request):
        def middleware(requests_info) -> list:
            if len(requests_info) == 0:
                # early return if batch to request is empty
                return []

            if len(requests_info) > self._w3.rpc_batch_max_size != 0:
                response = []
                for start in range(0, len(requests_info), self._w3.rpc_batch_max_size):
                    response += middleware(requests_info[start:start + self._w3.rpc_batch_max_size])
                return response

            if self._w3.rpc_batch_max_size == 0 or len(requests_info) == 1:
                # if RPC does not support batch requests or single request in batch, make individual requests
                def request_wrapper(method, params):
                    response =  make_batch_request.__self__.make_request(method, params)
                    if "error" in response and self._w3.should_retry:
                        raise Exception(response["error"].get("message") or "Unknown RPC Error")
                    return response

                return [
                    exponential_retry(f"[batch]{method}")(request_wrapper)(
                        method,
                        params,
                        no_retry=not self._w3.should_retry
                    )
                    for method, params in requests_info
                ]

            try:
                response = make_batch_request(requests_info)
            except Exception as e:
                print(f"batch RPC call with {len(requests_info)} requests got exception {repr(e)}, splitting and retrying")
            else:
                if len(response) != len(requests_info):
                    print(f"made batch request with size {len(requests_info)} but only received {len(response)} results. splitting and retrying.{f' Sample response: {response[0]}' if len(response) != 0 else ''}")
                else:
                    # find individual failed requests
                    requests_retry = []
                    request_indexes: list[tuple[int, int]] = []
                    for i, (request_single, response_single) in enumerate(zip(requests_info, response)):
                        if (
                            "error" in response_single or
                            (
                                "eth_getBlockBy" in request_single[0] and
                                response_single.get("result") in NULL_RESPONSES
                            )
                        ):
                            request_indexes.append((i, len(requests_retry)))
                            requests_retry.append(request_single)

                    if len(requests_retry) == 0:
                        return response

                    print(f"{len(requests_retry)}/{len(requests_info)} requests in batch failed, retrying. Example response: {response[request_indexes[0][0]]}")

                    if len(requests_retry) != len(requests_info):  # if some requests succeeded, retry failed requests
                        response_new = middleware(requests_retry)
                        for old_idx, new_idx in request_indexes:
                            response[old_idx] = response_new[new_idx]
                        return response

            assert len(requests_info) > 1
            middle = len(requests_info) // 2
            sleep(0.1)
            return middleware(requests_info[:middle]) + middleware(requests_info[middle:])
        return middleware