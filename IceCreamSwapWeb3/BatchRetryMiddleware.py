from time import sleep

from web3.middleware import Web3Middleware

from IceCreamSwapWeb3 import Web3Advanced


class BatchRetryMiddleware(Web3Middleware):
    _w3: Web3Advanced

    def wrap_make_batch_request(self, make_batch_request):
        def middleware(requests_info) -> list:
            if len(requests_info) > self._w3.rpc_batch_max_size:
                response = []
                for start in range(0, len(requests_info), self._w3.rpc_batch_max_size):
                    response += middleware(requests_info[start:start + self._w3.rpc_batch_max_size])
                return response

            try:
                response = make_batch_request(requests_info)
            except Exception as e:
                if len(requests_info) == 1:
                    print(f"batch RPC call with single request got exception {repr(e)}, waiting 1 sec and trying again")
                    sleep(1)
                    return middleware(requests_info)
                print(f"batch RPC call with {len(requests_info)} requests got exception {repr(e)}, splitting and retrying")
            else:
                if len(response) == len(requests_info):
                    # find individual failed requests
                    requests_retry = []
                    request_indexes: list[tuple[int, int]] = []
                    for i, (request_single, response_single) in enumerate(zip(requests_info, response)):
                        if "error" in response_single or response_single["result"] is None:
                            request_indexes.append((i, len(requests_retry)))
                            requests_retry.append(request_single)

                    if len(requests_retry) != 0:
                        # retry failed requests
                        print(f"{len(requests_retry)}/{len(requests_info)} requests in batch failed, retrying. Example response: {response[request_indexes[0][0]]}")
                        if len(requests_retry) == len(requests_info):
                            # all failed, let's wait a moment before retrying
                            sleep(1)
                        response_new = middleware(requests_retry)
                        for old_idx, new_idx in request_indexes:
                            response[old_idx] = response_new[new_idx]

                    return response
            middle = len(requests_info) // 2
            return middleware(requests_info[:middle]) + middleware(requests_info[middle:])
        return middleware