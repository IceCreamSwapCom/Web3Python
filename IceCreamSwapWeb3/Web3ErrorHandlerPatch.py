import warnings

from eth_abi import abi
from eth_utils import to_bytes
from web3._utils.error_formatters_utils import (
    SOLIDITY_ERROR_FUNC_SELECTOR,
    ContractLogicError, OFFCHAIN_LOOKUP_FUNC_SELECTOR, OFFCHAIN_LOOKUP_FIELDS, PANIC_ERROR_FUNC_SELECTOR,
    PANIC_ERROR_CODES, MISSING_DATA
)
from web3.exceptions import OffchainLookup, ContractPanicError, ContractCustomError
from web3.types import RPCResponse


def _parse_error_with_reverted_prefix(data: str) -> str:
    """
    Parse errors from the data string which begin with the "Reverted" prefix or the Reverted function selector.
    "Reverted", function selector and offset are always the same for revert errors
    """
    if data.startswith("Reverted "):
        data = data.replace("Reverted ", "")

    data_offset = ("00" * 31) + "20"  # 0x0000...0020 (32 bytes)
    revert_pattern = SOLIDITY_ERROR_FUNC_SELECTOR + data_offset

    error = data
    if data.startswith(revert_pattern):
        # if common revert pattern
        string_length = int(data[len(revert_pattern) : len(revert_pattern) + 64], 16)
        error = data[
            len(revert_pattern) + 64 : len(revert_pattern) + 64 + string_length * 2
        ]
    elif data.startswith("0x"):
        # Special case for this form: '0x...'
        error = data.split(" ")[0][2:]

    try:
        error = bytes.fromhex(error).decode("utf8")
    except UnicodeDecodeError:
        warnings.warn(
            "Could not decode revert reason as UTF-8", RuntimeWarning, stacklevel=2
        )
        raise ContractLogicError("execution reverted", data=data)

    return error


def _raise_contract_error(response_error_data: str) -> None:
    """
    Decode response error from data string and raise appropriate exception.

        "Reverted " (prefix may be present in `data`)
        Function selector for Error(string): 08c379a (4 bytes)
        Data offset: 32 (32 bytes)
        String length (32 bytes)
        Reason string (padded, use string length from above to get meaningful part)
    """
    if response_error_data.startswith("Reverted ") or response_error_data[:10] == SOLIDITY_ERROR_FUNC_SELECTOR:
        reason_string = _parse_error_with_reverted_prefix(response_error_data)
        raise ContractLogicError(
            f"execution reverted: {reason_string}", data=response_error_data
        )

    elif response_error_data[:10] == OFFCHAIN_LOOKUP_FUNC_SELECTOR:
        # --- EIP-3668 | CCIP read error --- #
        parsed_data_as_bytes = to_bytes(hexstr=response_error_data[10:])
        abi_decoded_data = abi.decode(
            list(OFFCHAIN_LOOKUP_FIELDS.values()), parsed_data_as_bytes
        )
        offchain_lookup_payload = dict(
            zip(OFFCHAIN_LOOKUP_FIELDS.keys(), abi_decoded_data)
        )
        raise OffchainLookup(offchain_lookup_payload, data=response_error_data)

    elif response_error_data[:10] == PANIC_ERROR_FUNC_SELECTOR:
        # --- Solidity Panic Error --- #
        panic_error_code = response_error_data[-2:]
        raise ContractPanicError(
            PANIC_ERROR_CODES[panic_error_code], data=response_error_data
        )

    # Solidity 0.8.4 introduced custom error messages that allow args to
    # be passed in (or not). See:
    # https://blog.soliditylang.org/2021/04/21/custom-errors/
    elif (len(response_error_data) >= 10):
        # Raise with data as both the message and the data for backwards
        # compatibility and so that data can be accessed via 'data' attribute
        # on the ContractCustomError exception
        raise ContractCustomError(response_error_data, data=response_error_data)


def raise_contract_logic_error_on_revert(response: RPCResponse) -> RPCResponse:
    """
    Revert responses contain an error with the following optional attributes:
        `code` - in this context, used for an unknown edge case when code = '3'
        `message` - error message is passed to the raised exception
        `data` - response error details (str, dict, None)

    See also https://solidity.readthedocs.io/en/v0.6.3/control-structures.html#revert
    """
    error = response.get("error")
    if error is None or isinstance(error, str):
        raise ValueError(error)

    message = error.get("message")
    message_present = message is not None and message != ""
    data = error.get("data")

    if data is None:
        if message_present:
            raise ContractLogicError(message, data=MISSING_DATA)
        elif not message_present:
            raise ContractLogicError("execution reverted", data=MISSING_DATA)
    elif isinstance(data, dict) and message_present:
        raise ContractLogicError(f"execution reverted: {message}", data=data)
    elif isinstance(data, str):
        _raise_contract_error(data)

    if message_present:
        # Geth Revert with error message and code 3 case:
        if error.get("code") == 3:
            raise ContractLogicError(message, data=data)
        elif "revert" in message.lower():
            raise ContractLogicError(message, data=data)

    return response


def patch_error_formatters():
    import web3._utils.error_formatters_utils as old_error_formatters_utils
    from web3._utils.method_formatters import ERROR_FORMATTERS
    from web3._utils.rpc_abi import RPC

    old_error_formatters_utils.raise_contract_logic_error_on_revert = raise_contract_logic_error_on_revert
    ERROR_FORMATTERS[RPC.eth_estimateGas] = raise_contract_logic_error_on_revert
    ERROR_FORMATTERS[RPC.eth_call] = raise_contract_logic_error_on_revert
