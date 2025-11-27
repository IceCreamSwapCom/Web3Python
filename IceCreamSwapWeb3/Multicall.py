import copy
from importlib.resources import files
from typing import Optional

import eth_abi
from eth_typing import BlockIdentifier
from eth_utils import to_bytes
from eth_utils.abi import get_abi_output_types, get_abi_input_types
from web3.contract.contract import ContractFunction, ContractConstructor
from web3.exceptions import ContractLogicError
from web3.types import StateOverride

from .AddressCalculator import calculate_create_address
from .FastChecksumAddress import to_checksum_address

# load multicall abi
with files("IceCreamSwapWeb3").joinpath("./abi/Multicall.abi").open('r') as f:
    MULTICALL_ABI = f.read()

# load undeployed multicall abi and bytecode
with files("IceCreamSwapWeb3").joinpath("./abi/UndeployedMulticall.abi").open('r') as f:
    UNDEPLOYED_MULTICALL_ABI = f.read()
with files("IceCreamSwapWeb3").joinpath("./bytecode/UndeployedMulticall.bytecode").open('r') as f:
    UNDEPLOYED_MULTICALL_BYTECODE = f.read()

# allowed chars in HEX string
HEX_CHARS = set("0123456789abcdef")


class MultiCall:
    CALLER_ADDRESS = "0x0000000000000000000000000000000000000123"

    MULTICALL_DEPLOYMENTS: dict[int, str] = {
        1116: "0x66BF74f6Afe41fd10a5343B39Dae017EF9CceF1b",
        245022934: "0x8179Cb0771B8CAA1ef412266e9faCe0C8d05E4Db",
        40: "0x84C2fb5A4F7219688AF475e74b2ac189966Cc9ba",
        56: "0xa814Cfa623C1d26cAcB18a2aCd9129f71D461dd1",
        8453: "0xdB1342b15E188Ca3B5Bc327552299FDf643212C2",
        42161: "0x1E0b5202F8D4a247d12528ac865ab73C61Db35Af",
        71402: "0xf8ac4BEB2F75d2cFFb588c63251347fdD629B92c",
        88: "0xf8ac4BEB2F75d2cFFb588c63251347fdD629B92c",
    }

    @classmethod
    def register_multicall_contract(cls, chain_id: int, contract_address: str):
        cls.MULTICALL_DEPLOYMENTS[chain_id] = to_checksum_address(contract_address)

    def __init__(
            self,
            w3: "Web3Advanced"
    ):
        self.w3 = w3
        self.chain_id = self.w3.eth.chain_id

        if self.chain_id in self.MULTICALL_DEPLOYMENTS:
            self.multicall = self.w3.eth.contract(
                abi=MULTICALL_ABI,
                address=to_checksum_address(self.MULTICALL_DEPLOYMENTS[self.chain_id])
            )
            self.undeployed_contract_address = calculate_create_address(sender=self.multicall.address, nonce=1)
        else:
            self.multicall = self.w3.eth.contract(abi=UNDEPLOYED_MULTICALL_ABI, bytecode=UNDEPLOYED_MULTICALL_BYTECODE)
            self.undeployed_contract_address = self.calculate_expected_contract_address(sender=self.CALLER_ADDRESS, nonce=0)

        self.calls: list[ContractFunction] = []
        self.state_overwrites: list[StateOverride | None] = []
        self.undeployed_contract_constructor: Optional[ContractConstructor] = None

    def add_call(self, contract_func: ContractFunction, state_override: Optional[StateOverride] = None):
        self.calls.append(contract_func)
        self.state_overwrites.append(state_override)

    def add_undeployed_contract(self, contract_constructor: ContractConstructor):
        assert self.undeployed_contract_constructor is None, "can only add one undeployed contract"
        self.undeployed_contract_constructor = contract_constructor

    def add_undeployed_contract_call(self, contract_func: ContractFunction, state_override: Optional[StateOverride] = None):
        assert self.undeployed_contract_constructor is not None, "No undeployed contract added yet"
        contract_func = copy.copy(contract_func)
        contract_func.address = 0  # self.undeployed_contract_address
        self.calls.append(contract_func)
        self.state_overwrites.append(state_override)

    def call(
            self,
            use_revert: Optional[bool] = None,
            batch_size: int = 1_000,
            state_override: Optional[StateOverride] = None,
            block_identifier: Optional[BlockIdentifier] = None
    ):
        results, _ = self.call_with_gas(
            use_revert=use_revert,
            batch_size=batch_size,
            state_override=state_override,
            block_identifier=block_identifier
        )
        return results

    def call_with_gas(
            self,
            use_revert: Optional[bool] = None,
            batch_size: int = 1_000,
            state_override: Optional[StateOverride] = None,
            block_identifier: Optional[BlockIdentifier] = None
    ):
        if state_override is not None:
            assert self.w3.overwrites_available
        if use_revert is None:
            use_revert = self.w3.revert_reason_available

        calls = self.calls
        state_overwrites = self.state_overwrites
        calls_with_calldata = self.add_calls_calldata(calls)

        return self._inner_call(
            use_revert=use_revert,
            calls_with_calldata=calls_with_calldata,
            batch_size=batch_size,
            state_overwrites=state_overwrites,
            global_state_override=state_override,
            block_identifier=block_identifier,
        )

    def _inner_call(
            self,
            use_revert: bool,
            calls_with_calldata: list[tuple[ContractFunction, bytes]],
            batch_size: int,
            state_overwrites: list[StateOverride | None],
            global_state_override: StateOverride | None = None,
            block_identifier: Optional[BlockIdentifier] = None
    ) -> tuple[list[Exception | tuple], list[int]]:
        assert len(calls_with_calldata) == len(state_overwrites)
        if len(calls_with_calldata) == 0:
            return [], []
        kwargs = dict(
            use_revert=use_revert,
            batch_size=batch_size,
            global_state_override=global_state_override,
            block_identifier=block_identifier
        )
        # make sure calls are not bigger than batch_size
        if len(calls_with_calldata) > batch_size:
            results_combined = []
            gas_usages_combined = []
            for start in range(0, len(calls_with_calldata), batch_size):
                end = min(start + batch_size, len(calls_with_calldata))
                results, gas_usages = self._inner_call(
                    **kwargs,
                    calls_with_calldata=calls_with_calldata[start:end],
                    state_overwrites=state_overwrites[start:end]
                )
                results_combined += results
                gas_usages_combined += gas_usages
            return results_combined, gas_usages_combined

        if self.multicall.address is None:
            multicall_call = self._build_constructor_calldata(
                calls_with_calldata=calls_with_calldata,
                use_revert=use_revert
            )
        else:
            multicall_call = self._build_calldata(
                calls_with_calldata=calls_with_calldata
            )
            use_revert = False

        state_override = self.merge_state_overwrites([global_state_override] + state_overwrites)

        try:
            raw_returns, gas_usages = self._call_multicall(
                multicall_call=multicall_call,
                use_revert=use_revert,
                retry=False,
                state_override=state_override,
                block_identifier=block_identifier
            )
        except Exception:
            if len(calls_with_calldata) == 1:
                try:
                    raw_returns, gas_usages = self._call_multicall(
                        multicall_call=multicall_call,
                        use_revert=use_revert,
                        retry=self.w3.should_retry,
                        state_override=state_override,
                        block_identifier=block_identifier
                    )
                except Exception as e:
                    print(f"Single multicall to {calls_with_calldata[0][0].address} and func {calls_with_calldata[0][0].signature} got Error: {repr(e)}")
                    raw_returns = [e]
                    gas_usages = [None]
            else:
                middle = len(calls_with_calldata) // 2
                left_results, left_gas_usages = self._inner_call(
                    **kwargs,
                    calls_with_calldata=calls_with_calldata[:middle],
                    state_overwrites=state_overwrites[:middle]
                )
                right_results, right_gas_usages = self._inner_call(
                    **kwargs,
                    calls_with_calldata=calls_with_calldata[middle:],
                    state_overwrites=state_overwrites[middle:]
                )
                return left_results + right_results, left_gas_usages + right_gas_usages
        else:
            if len(raw_returns) != len(calls_with_calldata) and len(raw_returns) > 1:
                # multicall stopped in the middle due to running out of gas.
                # better remove the last result.
                raw_returns = raw_returns[:-1]
                gas_usages = gas_usages[:-1]
        assert len(raw_returns) == len(gas_usages)
        results = self.decode_contract_function_results(raw_returns=raw_returns, contract_functions=[call for call, _ in calls_with_calldata])
        if len(results) == len(calls_with_calldata):
            return results, gas_usages
        # if not all calls were executed, recursively execute remaining calls and concatenate results
        right_results, right_gas_usages = self._inner_call(
            **kwargs,
            calls_with_calldata=calls_with_calldata[len(results):],
            state_overwrites=state_overwrites[len(results):]
        )
        return results + right_results, gas_usages + right_gas_usages

    @staticmethod
    def merge_state_overwrites(state_overwrites: list[StateOverride | None]) -> StateOverride | None:
        if all(overwrite is None for overwrite in state_overwrites):
            return None
        merged_overwrite: StateOverride = {}
        for overwrites in state_overwrites:
            if overwrites is None:
                continue
            for contract_address, overwrite in overwrites.items():
                if contract_address not in merged_overwrite:
                    merged_overwrite[contract_address] = copy.deepcopy(overwrite)
                    continue
                prev_overwrite = merged_overwrite[contract_address]
                if "balance" in overwrite:
                    assert "balance" not in prev_overwrite
                    prev_overwrite["balance"] = overwrite["balance"]
                if "nonce" in overwrite:
                    assert "nonce" not in prev_overwrite
                    prev_overwrite["nonce"] = overwrite["nonce"]
                if "code" in overwrite:
                    assert "code" not in prev_overwrite
                    prev_overwrite["code"] = overwrite["code"]
                if "state" in overwrite:
                    assert "state" not in prev_overwrite
                    assert "stateDiff" not in prev_overwrite
                    prev_overwrite["state"] = overwrite["state"]
                if "stateDiff" in overwrite:
                    assert "state" not in prev_overwrite
                    if "stateDiff" not in prev_overwrite:
                        prev_overwrite["stateDiff"] = copy.deepcopy(overwrite["stateDiff"])
                    else:
                        prev_state_diff = prev_overwrite["stateDiff"]
                        for slot, value in overwrite["stateDiff"].items():
                            assert slot not in prev_state_diff or prev_state_diff[slot] == value
                            prev_state_diff[slot] = value
        return merged_overwrite

    @staticmethod
    def calculate_expected_contract_address(sender: str, nonce: int):
        undeployed_contract_runner_address = calculate_create_address(sender=sender, nonce=nonce)
        contract_address = calculate_create_address(sender=undeployed_contract_runner_address, nonce=1)
        return contract_address

    @staticmethod
    def add_calls_calldata(calls: list[ContractFunction]) -> list[tuple[ContractFunction, bytes]]:
        calls_with_calldata = []
        for call in calls:
            function_abi = get_abi_input_types(call.abi)
            assert len(function_abi) == len(call.arguments)
            function_args = []
            for aby_type, arg in zip(function_abi, call.arguments):
                if aby_type == "bytes":
                    arg = to_bytes(hexstr=arg)
                function_args.append(arg)
            call_data = to_bytes(hexstr=call.selector) + eth_abi.encode(function_abi, function_args)
            calls_with_calldata.append((call, call_data))
        assert len(calls_with_calldata) == len(calls)
        return calls_with_calldata

    def _build_calldata(self, calls_with_calldata: list[tuple[ContractFunction, bytes]]) -> ContractFunction:
        assert self.multicall.address is not None

        if self.undeployed_contract_constructor is not None:
            # deploy undeployed contract first and then call the other functions
            contract_deployment_call = self.multicall.functions.deployContract(
                contractBytecode=to_bytes(hexstr=self.undeployed_contract_constructor.data_in_transaction)
            )
            contract_deployment_calldata = to_bytes(hexstr=contract_deployment_call.selector) + \
                                           eth_abi.encode(
                                               get_abi_input_types(contract_deployment_call.abi),
                                               contract_deployment_call.arguments
                                           )
            # contract_deployment_calldata = to_bytes(hexstr=contract_deployment_call._encode_transaction_data())
            calls_with_calldata = [(contract_deployment_call, contract_deployment_calldata)] + calls_with_calldata

        encoded_calls = []
        for call, call_data in calls_with_calldata:
            to_address = call.address if call.address != 0 else self.undeployed_contract_address
            encoded_calls.append((to_address, 100_000_000, call_data))  # target, gasLimit, callData

        # build multicall transaction
        multicall_call = self.multicall.functions.multicallWithGasLimitation(
            calls=encoded_calls,
            gasBuffer=1_000_000,
        )

        # return multicall address and calldata
        return multicall_call

    def _build_constructor_calldata(
            self,
            calls_with_calldata: list[tuple[ContractFunction, bytes]],
            use_revert: bool
    ) -> ContractConstructor:
        assert self.multicall.address is None

        # Encode the number of calls as the first 32 bytes
        number_of_calls = len(calls_with_calldata)
        encoded_calls = eth_abi.encode(['uint256'], [number_of_calls]).hex()

        previous_target = None
        previous_call_data = None

        for call, call_data in calls_with_calldata:
            target = call.address if call.address != 0 else "0x0000000000000000000000000000000000000000"

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
                call_data_encoded = call_data.hex()
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
            encodedCalls=to_bytes(hexstr=encoded_calls)
        )

        return multicall_call

    @staticmethod
    def _decode_muilticall(
            multicall_result: bytes | list[tuple[bool, int, bytes]]
    ) -> tuple[list[str | Exception], list[int]]:
        raw_returns: list[str | Exception] = []
        gas_usages: list[int] = []

        if isinstance(multicall_result, list) or isinstance(multicall_result, tuple):
            # deployed multicall
            for sucess, gas_usage, raw_return in multicall_result:
                if not sucess:
                    decoded = MultiCall.get_revert_reason(raw_return)
                    raw_return = ContractLogicError(f"execution reverted: {decoded}")
                raw_returns.append(raw_return)
                gas_usages.append(gas_usage)
            return raw_returns, gas_usages

        # undeployed multicall
        # decode returned data into segments
        multicall_result_copy = multicall_result[:]
        raw_returns_encoded = []
        while len(multicall_result_copy) != 0:
            data_len = int.from_bytes(multicall_result_copy[:2], byteorder='big')
            raw_returns_encoded.append(multicall_result_copy[2:data_len])
            multicall_result_copy = multicall_result_copy[data_len:]

        # decode returned data for each call
        for raw_return_encoded in raw_returns_encoded:
            try:
                # we are using packed encoding to decrease size of return data, if not we could have used
                # success, raw_return = eth_abi.decode(['bool', 'bytes'], raw_return_encoded)
                success = raw_return_encoded[0] == 1
                gas_usage = int(raw_return_encoded[1:5].hex(), 16)
                raw_return = raw_return_encoded[5:]
                if not success:
                    decoded = MultiCall.get_revert_reason(raw_return)
                    raw_return = ContractLogicError(f"execution reverted: {decoded}")
            except Exception as e:
                raw_return = e
                gas_usage = None
            gas_usages.append(gas_usage)
            raw_returns.append(raw_return)
        return raw_returns, gas_usages

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
                return "0x" + revert_bytes.hex()

    def _call_multicall(
            self,
            multicall_call: ContractConstructor | ContractFunction,
            use_revert: bool,
            retry: bool = False,
            state_override: Optional[StateOverride] = None,
            block_identifier: Optional[BlockIdentifier] = None
    ):
        # call transaction
        try:
            if isinstance(multicall_call, ContractConstructor):
                multicall_result = self.w3.eth.call({
                    "from": self.CALLER_ADDRESS,
                    "nonce": 0,
                    "data": multicall_call.data_in_transaction,
                    "no_retry": not retry,
                },
                    state_override=state_override,
                    block_identifier=block_identifier
                )
            else:
                assert isinstance(multicall_call, ContractFunction)
                # manually encoding and decoding call because web3.py is sooooo slow...
                # The simple but slow version is as below:
                # _, multicall_result, completed_calls = multicall_call.call({
                #     "from": self.CALLER_ADDRESS,
                #     "nonce": 0,
                #     "no_retry": not retry,
                # })

                calldata = to_bytes(hexstr=multicall_call.selector) + \
                           eth_abi.encode(get_abi_input_types(multicall_call.abi), multicall_call.arguments)
                raw_response = self.w3.eth.call({
                    "from": self.CALLER_ADDRESS,
                    "to": multicall_call.address,
                    "nonce": 0,
                    "data": calldata,
                    "no_retry": not retry,
                },
                    state_override=state_override,
                    block_identifier=block_identifier
                )
                _, multicall_result = eth_abi.decode(get_abi_output_types(multicall_call.abi), raw_response)

                if len(multicall_result) > 0 and self.undeployed_contract_constructor is not None:
                    # remove first call result as that's the deployment of the undeployed contract
                    success, _, address_encoded = multicall_result[0]
                    assert success, "Undeployed contract constructor reverted"
                    assert "0x" + address_encoded[-20:].hex() == self.undeployed_contract_address.lower(), "unexpected undeployed contract address"
                    multicall_result = multicall_result[1:]
            if use_revert:
                raise ValueError("Multicall did not revert but was expected to")
        except ContractLogicError as e:
            if not use_revert:
                raise
            if not e.message.startswith("execution reverted: "):
                raise
            result_str = e.message.removeprefix("execution reverted: ")
            if any((char not in HEX_CHARS for char in result_str)):
                raise
            multicall_result = to_bytes(hexstr=result_str)

        if len(multicall_result) == 0:
            raise ValueError("No data returned from multicall")

        return self._decode_muilticall(multicall_result)

    @staticmethod
    def decode_contract_function_result(raw_return: str | Exception, contract_function: ContractFunction):
        if isinstance(raw_return, Exception):
            return raw_return
        try:
            result = eth_abi.decode(get_abi_output_types(contract_function.abi), raw_return)
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
    from IceCreamSwapWeb3 import Web3Advanced

    w3 = Web3Advanced(node_url=node_url)

    with files("IceCreamSwapWeb3").joinpath("./abi/Counter.abi").open('r') as f:
        counter_contract_abi = f.read()
    with files("IceCreamSwapWeb3").joinpath("./bytecode/Counter.bytecode").open('r') as f:
        counter_contract_bytecode = f.read()
    with files("IceCreamSwapWeb3").joinpath("./abi/ERC20.abi").open('r') as f:
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

    multicall_results, gas_usages = multicall.call_with_gas()
    print(list(zip(multicall_results, gas_usages)))


if __name__ == "__main__":
    main()
