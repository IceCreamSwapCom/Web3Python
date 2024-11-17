import unittest
from unittest.mock import MagicMock, patch
from . import Web3Advanced


class TestWeb3AdvancedGetLogs(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Instantiate the class only once
        cls.eth_advanced = Web3Advanced(node_url="https://rpc-core.icecreamswap.com").eth

    def setUp(self):
        # Mock self.eth_advanced.w3 and its properties
        self.eth_advanced.w3 = MagicMock()
        self.eth_advanced.w3.filter_block_range = 1000  # Set a default filter block range
        self.eth_advanced.w3.unstable_blocks = 10       # Set default unstable blocks
        self.eth_advanced.w3.latest_seen_block = 1000   # Set the latest seen block

        # Mock get_block_number
        self.eth_advanced.get_block_number = MagicMock(return_value=1000)

        # Mock get_block
        def mock_get_block(block_identifier, no_retry=False):
            if isinstance(block_identifier, int):
                block_number = block_identifier
            elif block_identifier == 'latest':
                block_number = 1000
            else:
                # Handle other block identifiers as needed
                block_number = None
            if block_number is None:
                raise Exception(f"Invalid block identifier: {block_identifier}")
            block_hash = f"hash_{block_number}"
            parent_hash = f"hash_{block_number - 1}" if block_number > 0 else None
            return {'number': block_number, 'hash': block_hash, 'parentHash': parent_hash}
        self.eth_advanced.get_block = MagicMock(side_effect=mock_get_block)

        # Mock _get_logs
        self.logs_storage = {}  # To simulate storage of logs per block
        def mock__get_logs(filter_params):
            if 'blockHash' in filter_params:
                # Single block query
                block_number = int(filter_params['blockHash'].split('_')[1])
                logs = self.logs_storage.get(block_number, [])
                return logs
            else:
                from_block = filter_params.get('fromBlock', 0)
                to_block = filter_params.get('toBlock', 0)
                logs = []
                for block_number in range(from_block, to_block + 1):
                    block_logs = self.logs_storage.get(block_number, [])
                    logs.extend(block_logs)
                return logs
        self.eth_advanced._get_logs = MagicMock(side_effect=mock__get_logs)

    def test_get_logs_no_duplicates_no_missing_blocks_correct_order(self):
        # Prepare test data
        from_block = 900
        to_block = 950
        filter_params = {'fromBlock': from_block, 'toBlock': to_block}

        # Simulate logs for each block
        for block_number in range(from_block, to_block + 1):
            self.logs_storage[block_number] = [{'blockNumber': block_number, 'logIndex': 0}]

        # Call get_logs
        logs = self.eth_advanced.get_logs(filter_params, use_subsquid=False)

        # Collect block numbers from logs
        actual_block_numbers = [log['blockNumber'] for log in logs]
        expected_block_numbers = list(range(from_block, to_block + 1))

        # Assertions
        self.assertEqual(len(actual_block_numbers), len(set(actual_block_numbers)), "Duplicate logs found")
        self.assertEqual(sorted(actual_block_numbers), expected_block_numbers, "Missing or extra logs found")
        self.assertEqual(actual_block_numbers, expected_block_numbers, "Logs are not in correct order")

    def test_get_logs_range_exceeds_filter_block_range(self):
        # Adjust filter_block_range to force splitting
        self.eth_advanced.w3.filter_block_range = 10

        # Prepare test data
        from_block = 50
        to_block = 100
        filter_params = {'fromBlock': from_block, 'toBlock': to_block}

        # Simulate logs for each block
        for block_number in range(from_block, to_block + 1):
            self.logs_storage[block_number] = [{'blockNumber': block_number, 'logIndex': 0}]

        # Call get_logs
        logs = self.eth_advanced.get_logs(filter_params, use_subsquid=False)

        # Collect block numbers from logs
        actual_block_numbers = [log['blockNumber'] for log in logs]
        expected_block_numbers = list(range(from_block, to_block + 1))

        # Assertions
        self.assertEqual(len(actual_block_numbers), len(set(actual_block_numbers)), "Duplicate logs found")
        self.assertEqual(sorted(actual_block_numbers), expected_block_numbers, "Missing or extra logs found")
        self.assertEqual(actual_block_numbers, expected_block_numbers, "Logs are not in correct order")

    def test_get_logs_splits_on_error(self):
        # Prepare test data
        from_block = 50
        to_block = 100
        filter_params = {'fromBlock': from_block, 'toBlock': to_block}

        for block_number in range(from_block, to_block + 1):
            self.logs_storage[block_number] = [{'blockNumber': block_number, 'logIndex': 0}]

        # Simulate logs for each block except when more than 10 logs are requested, then raise exception
        def mock__get_logs(filter_params):
            from_block = filter_params.get('fromBlock', 0)
            to_block = filter_params.get('toBlock', 0)
            if to_block - from_block + 1 > 10:
                raise Exception("Simulated RPC error")
            logs = []
            for block_number in range(from_block, to_block + 1):
                logs.extend(self.logs_storage.get(block_number, []))
            return logs
        self.eth_advanced._get_logs.side_effect = mock__get_logs

        # Call get_logs
        logs = self.eth_advanced.get_logs(filter_params, use_subsquid=False)

        # Collect block numbers from logs
        actual_block_numbers = [log['blockNumber'] for log in logs]
        expected_block_numbers = list(range(from_block, to_block + 1))

        # Assertions
        self.assertEqual(len(actual_block_numbers), len(set(actual_block_numbers)), "Duplicate logs found")
        self.assertEqual(sorted(actual_block_numbers), expected_block_numbers, "Missing or extra logs found")
        self.assertEqual(actual_block_numbers, expected_block_numbers, "Logs are not in correct order")

    @patch('IceCreamSwapWeb3.EthAdvanced.get_filter')
    def test_get_logs_uses_subsquid(self, mock_get_filter):
        # Prepare test data
        from_block = 800
        to_block = 850
        filter_params = {'fromBlock': from_block, 'toBlock': to_block}

        for block_number in range(to_block - 10, to_block + 1):
            self.logs_storage[block_number] = [{'blockNumber': block_number, 'logIndex': 0}]

        # Simulate logs returned by get_filter
        def mock_get_filter_func(chain_id, filter_params, partial_allowed, p_bar):
            till_block = to_block - 10
            logs = []
            for block_number in range(from_block, till_block + 1):
                logs.append({'blockNumber': block_number, 'logIndex': 0})
            return till_block, logs
        mock_get_filter.side_effect = mock_get_filter_func

        # Call get_logs with use_subsquid=True
        logs = self.eth_advanced.get_logs(filter_params, use_subsquid=True)

        # Collect block numbers from logs
        actual_block_numbers = [log['blockNumber'] for log in logs]
        expected_block_numbers = list(range(from_block, to_block + 1))

        # Assertions
        self.assertEqual(len(actual_block_numbers), len(set(actual_block_numbers)), "Duplicate logs found")
        self.assertEqual(sorted(actual_block_numbers), expected_block_numbers, "Missing or extra logs found")
        self.assertEqual(actual_block_numbers, expected_block_numbers, "Logs are not in correct order")

    def test_get_logs_unstable_blocks_handling(self):
        # Prepare test data where to_block is within the latest unstable blocks
        unstable_blocks = self.eth_advanced.w3.unstable_blocks
        latest_block = self.eth_advanced.get_block_number()
        from_block = latest_block - unstable_blocks - 5
        to_block = latest_block  # This will be within the unstable blocks

        filter_params = {'fromBlock': from_block, 'toBlock': to_block}

        # Simulate logs for each block
        for block_number in range(from_block, to_block + 1):
            self.logs_storage[block_number] = [{'blockNumber': block_number, 'logIndex': 0}]

        # Ensure get_block returns consistent block hashes and parent hashes
        def mock_get_block(block_identifier, no_retry=False):
            if isinstance(block_identifier, int):
                block_number = block_identifier
            elif block_identifier == 'latest':
                block_number = latest_block
            else:
                # Handle other block identifiers as needed
                block_number = None
            if block_number is None:
                raise Exception(f"Invalid block identifier: {block_identifier}")
            block_hash = f"hash_{block_number}"
            parent_hash = f"hash_{block_number - 1}" if block_number > 0 else None
            return {'number': block_number, 'hash': block_hash, 'parentHash': parent_hash}

        self.eth_advanced.get_block.side_effect = mock_get_block

        # Mock get_logs_inner to simulate fetching logs by blockHash
        def mock_get_logs_inner(filter_params, no_retry=False):
            block_hash = filter_params.get('blockHash')
            if block_hash:
                block_number = int(block_hash.split('_')[1])
                return self.logs_storage.get(block_number, [])
            else:
                return []

        self.eth_advanced.get_logs_inner = MagicMock(side_effect=mock_get_logs_inner)

        # Call get_logs
        logs = self.eth_advanced.get_logs(filter_params, use_subsquid=False)

        # Collect block numbers from logs
        actual_block_numbers = [log['blockNumber'] for log in logs]
        expected_block_numbers = list(range(from_block, to_block + 1))

        # Assertions
        self.assertEqual(len(actual_block_numbers), len(set(actual_block_numbers)), "Duplicate logs found")
        self.assertEqual(sorted(actual_block_numbers), expected_block_numbers, "Missing or extra logs found")
        self.assertEqual(actual_block_numbers, expected_block_numbers, "Logs are not in correct order")


if __name__ == '__main__':
    unittest.main()
