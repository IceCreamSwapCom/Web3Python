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
                if len(response) == len(requests_info):
                    return response
            except Exception:
                if len(requests_info) == 1:
                    raise

            middle = len(requests_info) // 2
            return middleware(requests_info[:middle]) + middleware(requests_info[middle:])
        return middleware