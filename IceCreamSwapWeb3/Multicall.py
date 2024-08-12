import copy
from time import sleep
from typing import Optional

import eth_abi
import eth_utils
import rlp
from eth_utils import to_checksum_address, to_bytes
from web3._utils.abi import get_abi_output_types
from web3.contract.contract import ContractFunction, ContractConstructor
from web3.exceptions import ContractLogicError

from .Web3Advanced import Web3Advanced


# load multicall abi
with open("./abi/Multicall.abi") as f:
    MULTICALL_ABI = f.read()

# load undeployed multicall abi and bytecode
with open("./abi/UndeployedMulticall.abi") as f:
    UNDEPLOYED_MULTICALL_ABI = f.read()
with open("./bytecode/UndeployedMulticall.bytecode") as f:
    UNDEPLOYED_MULTICALL_BYTECODE = f.read()

# allowed chars in HEX string
HEX_CHARS = set("0123456789abcdef")


class MultiCall:
    CALLER_ADDRESS = "0x0000000000000000000000000000000000000123"

    MULTICALL_DEPLOYMENTS: dict[int, str] = {
        1116: "0x2C310a21E21a3eaDF4e53E1118aeD4614c51B576"
    }

    @classmethod
    def register_multicall_contract(cls, chain_id: int, contract_address: str):
        cls.MULTICALL_DEPLOYMENTS[chain_id] = to_checksum_address(contract_address)

    def __init__(
            self,
            w3: Web3Advanced
    ):
        self.w3 = copy.deepcopy(w3)
        self.chain_id = self.w3.eth.chain_id

        if self.chain_id in self.MULTICALL_DEPLOYMENTS:
            self.multicall = self.w3.eth.contract(
                abi=MULTICALL_ABI,
                address=to_checksum_address(self.MULTICALL_DEPLOYMENTS[self.chain_id])
            )
            self.undeployed_contract_address = self.calculate_create_address(sender=self.multicall.address, nonce=1)
        else:
            self.multicall = self.w3.eth.contract(abi=UNDEPLOYED_MULTICALL_ABI, bytecode=UNDEPLOYED_MULTICALL_BYTECODE)
            self.undeployed_contract_address = self.calculate_expected_contract_address(sender=self.CALLER_ADDRESS, nonce=0)

        self.calls: list[ContractFunction] = []
        self.undeployed_contract_constructor: Optional[ContractConstructor] = None

    def add_call(self, contract_func: ContractFunction):
        self.calls.append(contract_func)

    def add_undeployed_contract(self, contract_constructor: ContractConstructor):
        assert self.undeployed_contract_constructor is None, "can only add one undeployed contract"
        self.undeployed_contract_constructor = contract_constructor

    def add_undeployed_contract_call(self, contract_func: ContractFunction):
        assert self.undeployed_contract_constructor is not None, "No undeployed contract added yet"
        contract_func = copy.copy(contract_func)
        contract_func.address = self.undeployed_contract_address
        self.calls.append(contract_func)

    def call(self, use_revert: Optional[bool] = None, batch_size: int = 1_000):
        if use_revert is None:
            use_revert = self.w3.revert_reason_available

        return self._inner_call(use_revert=use_revert, calls=self.calls, batch_size=batch_size)

    def _inner_call(self, use_revert: bool, calls: list[ContractFunction], batch_size: int):
        # make sure calls are not bigger than batch_size
        if len(calls) > batch_size:
            results = []
            for start in range(0, len(calls), batch_size):
                results += self._inner_call(
                    use_revert=use_revert,
                    calls=calls[start: min(start + batch_size, len(calls))],
                    batch_size=batch_size
                )
            return results

        if self.multicall.address is None:
            multicall_call = self._build_constructor_calldata(calls=calls, use_revert=use_revert)
        else:
            multicall_call = self._build_calldata(calls=calls)
        try:
            raw_returns = self._call_multicall(
                multicall_call=multicall_call,
                retry=len(calls) == 1
            )
        except Exception as e:
            if len(calls) == 1:
                print(f"Multicall with single call got Exception '{repr(e)}', retrying in 1 sec")
                sleep(1)
                return self._inner_call(use_revert=use_revert, calls=calls)
            print(f"Multicall got Exception '{repr(e)}', splitting and retrying")
            left_results = self._inner_call(use_revert=use_revert, calls=calls[:len(calls) // 2])
            right_results = self._inner_call(use_revert=use_revert, calls=calls[len(calls) // 2:])
            return left_results + right_results
        results = self.decode_contract_function_results(raw_returns=raw_returns, contract_functions=calls)
        if len(results) == len(calls):
            return results
        # if not all calls were executed, recursively execute remaining calls and concatenate results
        return results + self._inner_call(use_revert=use_revert, calls=calls[len(results):])

    @staticmethod
    def calculate_expected_contract_address(sender: str, nonce: int):
        undeployed_contract_runner_address = MultiCall.calculate_create_address(sender=sender, nonce=nonce)
        contract_address = MultiCall.calculate_create_address(sender=undeployed_contract_runner_address, nonce=1)
        return contract_address

    @staticmethod
    def calculate_create_address(sender: str, nonce: int) -> str:
        assert len(sender) == 42
        sender_bytes = eth_utils.to_bytes(hexstr=sender)
        raw = rlp.encode([sender_bytes, nonce])
        h = eth_utils.keccak(raw)
        address_bytes = h[12:]
        return eth_utils.to_checksum_address(address_bytes)

    def _build_calldata(self, calls: list[ContractFunction]) -> ContractFunction:
        assert self.multicall.address is not None

        if self.undeployed_contract_constructor is not None:
            # deploy undeployed contract first and then call the other functions
            contract_deployment_call = self.multicall.functions.deployContract(
                contractBytecode=self.undeployed_contract_constructor.data_in_transaction
            )
            calls = [contract_deployment_call] + calls

        encoded_calls = []
        for call in calls:
            target = call.address
            call_data_hex = call._encode_transaction_data()
            call_data = to_bytes(hexstr=call_data_hex)

            encoded_calls.append({
                "target": target,
                "gasLimit": 100_000_000,
                "callData": call_data,
            })

        # build multicall transaction
        multicall_call = self.multicall.functions.multicallWithGasLimitation(
            calls=encoded_calls,
            gasBuffer=10_000_000,
        )

        # return multicall address and calldata
        return multicall_call

    def _build_constructor_calldata(self, calls: list[ContractFunction], use_revert: bool) -> ContractConstructor:
        assert self.multicall.address is None

        # Encode the number of calls as the first 32 bytes
        number_of_calls = len(calls)
        encoded_calls = eth_abi.encode(['uint256'], [number_of_calls]).hex()

        previous_target = None
        previous_call_data = None

        for call in calls:
            target = call.address
            call_data_hex = call._encode_transaction_data()
            call_data = to_bytes(hexstr=call_data_hex)

            # Determine the flags
            flags = 0
            if target == previous_target:
                flags |= 1  # Set bit 0 if target is the same as previous
            if call_data == previous_call_data:
                flags |= 2  # Set bit 1 if calldata is the same as previous

            # Encode the flag byte (1 byte)
            flags_encoded = format(flags, '02x')

            if flags & 1 == 0:  # If target is different
                # Encode target address (20 bytes, padded to 32 bytes)
                target_encoded = eth_abi.encode(['address'], [target]).hex()[24:]  # remove leading zeros
            else:
                target_encoded = ""

            if flags & 2 == 0:  # If calldata is different
                # Encode call data length (16 bits / 2 bytes)
                call_data_length_encoded = eth_abi.encode(['uint16'], [len(call_data)]).hex().zfill(4)[-4:]
                # Encode call data (variable length)
                call_data_encoded = call_data_hex[2:]
            else:
                call_data_length_encoded = ""
                call_data_encoded = ""

            encoded_calls += flags_encoded + target_encoded + call_data_length_encoded + call_data_encoded

            # Update previous values
            previous_target = target
            previous_call_data = call_data

        # build multicall transaction
        contract_constructor_data = bytes()
        if self.undeployed_contract_constructor is not None:
            contract_constructor_data = self.undeployed_contract_constructor.data_in_transaction
        multicall_call = self.multicall.constructor(
            useRevert=use_revert,
            contractBytecode=contract_constructor_data,
            encodedCalls=bytes.fromhex(encoded_calls)
        )

        return multicall_call

    @staticmethod
    def _decode_muilticall(multicall_result: bytes | list[tuple[bool, int, bytes]]) -> list[str | Exception]:
        raw_returns: list[str or Exception] = []

        if isinstance(multicall_result, list):
            # deployed multicall
            for sucess, _, raw_return in multicall_result:
                if not sucess:
                    decoded = MultiCall.get_revert_reason(raw_return)
                    raw_return = ContractLogicError(f"execution reverted: {decoded}")
                raw_returns.append(raw_return)
            return raw_returns

        # undeployed multicall
        # decode returned data into segments
        multicall_result_copy = multicall_result[:]
        raw_returns_encoded = []
        while len(multicall_result_copy) != 0:
            data_len = int.from_bytes(multicall_result_copy[:2], byteorder='big')
            raw_returns_encoded.append(multicall_result_copy[2:data_len+2])
            multicall_result_copy = multicall_result_copy[data_len+2:]

        # decode returned data for each call
        for raw_return_encoded in raw_returns_encoded:
            try:
                # we are using packed encoding to decrease size of return data, if not we could have used
                # success, raw_return = eth_abi.decode(['bool', 'bytes'], raw_return_encoded)
                success = raw_return_encoded[0] == 1
                raw_return = raw_return_encoded[1:]
                if not success:
                    decoded = MultiCall.get_revert_reason(raw_return)
                    raw_return = ContractLogicError(f"execution reverted: {decoded}")
            except Exception as e:
                raw_return = e
            raw_returns.append(raw_return)
        return raw_returns

    @staticmethod
    def get_revert_reason(revert_bytes: bytes) -> str:
        if len(revert_bytes) == 0:
            return "unknown"
        else:
            # first 4 bytes of revert code should be function selector for function Error(string)
            revert_bytes = revert_bytes[4:]
            try:
                return eth_abi.decode(['string'], revert_bytes)
            except Exception:
                return revert_bytes

    def _call_multicall(self, multicall_call: ContractConstructor | ContractFunction, retry: bool = False):
        # call transaction
        try:
            if isinstance(multicall_call, ContractConstructor):
                multicall_result = self.w3.eth.call({
                    "from": self.CALLER_ADDRESS,
                    "nonce": 0,
                    "data": multicall_call.data_in_transaction,
                    "no_retry": not retry,
                })
            else:
                assert isinstance(multicall_call, ContractFunction)
                _, multicall_result, _ = multicall_call.call({
                    "from": self.CALLER_ADDRESS,
                    "nonce": 0,
                    "no_retry": not retry,
                })
                if self.undeployed_contract_constructor is not None:
                    # remove first call result as that's the deployment of the undeployed contract
                    success, _, address_encoded = multicall_result[0]
                    assert success, "Undeployed contract constructor reverted"
                    assert "0x" + address_encoded[-20:].hex() == self.undeployed_contract_address.lower(), "unexpected undeployed contract address"
                    multicall_result = multicall_result[1:]
        except ContractLogicError as e:
            if not e.message.startswith("execution reverted: "):
                raise
            result_str = e.message.removeprefix("execution reverted: ")
            if any((char not in HEX_CHARS for char in result_str)):
                raise
            multicall_result = bytes.fromhex(result_str)

        if len(multicall_result) == 0:
            raise ValueError("No data returned from multicall")

        return self._decode_muilticall(multicall_result)

    @staticmethod
    def decode_contract_function_result(raw_return: str | Exception, contract_function: ContractFunction):
        if isinstance(raw_return, Exception):
            return raw_return
        try:
            output_types = get_abi_output_types(contract_function.abi)
            result = contract_function.w3.codec.decode(output_types, raw_return)
            if hasattr(result, "__len__") and len(result) == 1:
                result = result[0]
            return result
        except Exception as e:
            return e

    @staticmethod
    def decode_contract_function_results(raw_returns: list[str | Exception], contract_functions: list[ContractFunction]):
        return [MultiCall.decode_contract_function_result(raw_return, contract_function) for raw_return, contract_function in zip(raw_returns, contract_functions)]


def main(
        node_url="https://rpc-core.icecreamswap.com",
        usdt_address=to_checksum_address("0x900101d06A7426441Ae63e9AB3B9b0F63Be145F1"),
):
    w3 = Web3Advanced(node_url=node_url)

    with open("./abi/Counter.abi") as f:
        counter_contract_abi = f.read()
    with open("./bytecode/Counter.bytecode") as f:
        counter_contract_bytecode = f.read()
    with open("./abi/ERC20.abi") as f:
        erc20_abi = f.read()

    counter_contract = w3.eth.contract(bytecode=counter_contract_bytecode, abi=counter_contract_abi)
    usdt_contract = w3.eth.contract(address=usdt_address, abi=erc20_abi)

    # initializing new multicall
    multicall = w3.start_multicall()

    # calling an undeployed contract
    multicall.add_undeployed_contract(counter_contract.constructor(initialCounter=13))
    multicall.add_undeployed_contract_call(counter_contract.functions.counter())
    multicall.add_undeployed_contract_call(counter_contract.functions.updateCounter(newCounter=7))
    multicall.add_undeployed_contract_call(counter_contract.functions.counter())

    for _ in range(10_000):
        # calling a deployed contract
        multicall.add_call(usdt_contract.functions.decimals())

    multicall_result = multicall.call()
    print(multicall_result)


if __name__ == "__main__":
    main()
