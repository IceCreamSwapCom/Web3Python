from web3 import JSONBaseProvider

def patch_batching():
    # make sure batching attributes are instance attributes, not class level attributes

    original_init = JSONBaseProvider.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._is_batching = False
        self._batch_request_func_cache = (None, None)

    JSONBaseProvider.__init__ = patched_init