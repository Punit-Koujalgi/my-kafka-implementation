
from typing import Callable, Self, Protocol
import enum
from uuid import UUID

# --- typing declarations
class Readable(Protocol):
    def read(self, n: int, /) -> bytes:
        ...
'''
class Encodable(Protocol):
    def encode(self) -> bytes:
        ...

'''

type DecodeFunction[T] = Callable[[Readable], T]
type EncodeFunction[T] = Callable[[T], bytes]
#type EncodeFunction[T: Encodable] = Callable[[T], bytes]

# ---

@enum.unique
class ApiKey(enum.IntEnum):
    PRODUCE = 0
    FETCH = 1
    CREATE_TOPICS = 19
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75

    @classmethod
    def decode(cls, readable : Readable) -> Self:
        return cls(decode_int16(readable))

    def encode(self) -> bytes:
        return encode_int16(self)
    

@enum.unique
class ErrorCode(enum.IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    INVALID_TOPIC_EXCEPTION = 17
    TOPIC_ALREADY_EXISTS = 36
    UNSUPPORTED_VERSION = 35
    UNKNOWN_TOPIC_ID = 100

    def encode(self) -> bytes:
        return encode_int16(self)
    
    # decode not needed as we will only ever use this in response, never received in request


# -------  decoders and encoders begin ---------------

def decode_int8(readable: Readable) -> int:
    return int.from_bytes(readable.read(1), "big", signed=True) # big is default

def encode_int8(n: int) -> bytes:
    return n.to_bytes(1, "big", signed=True)

def decode_int16(readable: Readable) -> int:
    return int.from_bytes(readable.read(2), "big", signed=True)

def encode_int16(n: int) -> bytes:
    return n.to_bytes(2, "big", signed=True)

def decode_int32(readable: Readable) -> int:
    return int.from_bytes(readable.read(4), "big", signed=True)

def encode_int32(n: int) -> bytes:
    return n.to_bytes(4, "big", signed=True)

def decode_nullable_string(readable: Readable) -> str | None:
    n = decode_int16(readable)
    # correct? isn't 0 None and n - 1 logic
    return None if n < 0 else readable.read(n).decode("utf-8") # default
    #return None if n <= 0 else readable.read(n - 1).decode()

def encode_compact_nullable_string(s: str | None) -> bytes:
    if s is None:
        return encode_unsigned_varint(0)
    return encode_unsigned_varint(len(s) + 1) + s.encode()

def decode_compact_nullable_string(readable: Readable) -> str | None:
    n = decode_unsigned_varint(readable)
    if n == 0:
        return None
    return readable.read(n - 1).decode()

def decode_tagged_fields(readable: Readable) -> None:
    assert readable.read(1) == b'\x00', "Found unexpected tagged fields"  # Placeholder for tagged fields, currently not implemented

def encode_tagged_fields() -> bytes:
    return b'\x00'

def decode_unsigned_varint(readable: Readable) -> int:
    value = 0
    shift = 0
    while True:
        byte = decode_int8(readable)
        value |= (byte & 0x7F) << shift # value += (byte & 0x7F) << shift (works as well, note << has higher precedence than +=)
        if byte & 0x80 == 0:
            break
        shift += 7
    return value

def encode_unsigned_varint(n: int) -> bytes:
    encoding = b""
    while True:
        c, n = n & 0x7F, n >> 7
        if n > 0:
            c |= 0x80
        encoding += c.to_bytes(1)
        if n == 0:
            break
    return encoding
    

def decode_compact_string(readable: Readable) -> str:
    n = decode_unsigned_varint(readable)
    assert n > 0, "null compact string detected"
    return readable.read(n - 1).decode()

def encode_compact_string(s: str) -> bytes:
    n = len(s) + 1
    return encode_unsigned_varint(n) + s.encode()

def decode_compact_array[T](readable: Readable, decode_function: DecodeFunction[T]) -> list[T]:
    n = decode_unsigned_varint(readable)
    return [] if n == 0 else [decode_function(readable) for _ in range(n - 1)]

def encode_compact_array[T](items: list[T], encode_function: EncodeFunction[T] | None = None) -> bytes:
    n = len(items) + 1
    return encode_unsigned_varint(n) + \
        b"".join(encode_function(item) if encode_function else item.encode() for item in items) # type: ignore

def decode_uuid(readable: Readable) -> UUID:
    return UUID(bytes=readable.read(16))

def encode_uuid(u: UUID) -> bytes:
    return u.bytes

def decode_varint(readable: Readable) -> int:
    n = decode_unsigned_varint(readable)
    return -((n >> 1) + 1) if (n & 1) else (n >> 1)


def encode_varint(n: int) -> bytes:
    return encode_unsigned_varint((n << 1) ^ (n >> 31))


def decode_varlong(readable: Readable) -> int:
    return decode_varint(readable)


def encode_varlong(n: int) -> bytes:
    return encode_unsigned_varint((n << 1) ^ (n >> 63))

# -- From record_batch ---
def decode_int64(readable: Readable) -> int:
    return int.from_bytes(readable.read(8), signed=True)

def encode_int64(n: int) -> bytes:
    return n.to_bytes(8, signed=True)

def decode_uint32(readable: Readable) -> int:
    return int.from_bytes(readable.read(4))

def encode_uint32(n: int) -> bytes:
    return n.to_bytes(4)

def decode_array[T](readable: Readable, decode_function: DecodeFunction[T]) -> list[T]:
    n = decode_int32(readable)
    return [] if n < 0 else [decode_function(readable) for _ in range(n)]

def decode_compact_nullable_bytes(readable: Readable) -> bytes | None:
    n = decode_unsigned_varint(readable)
    if n == 0:
        return None
    length = n - 1
    data = readable.read(length)
    if len(data) < length:
        raise EOFError(f"Expected {length} bytes but got {len(data)} bytes while decoding compact nullable bytes")
    return data

def encode_array[T](arr: list[T], encode_function: EncodeFunction[T] | None = None) -> bytes:
    return encode_int32(len(arr)) + b"".join(
        t.encode() if encode_function is None else encode_function(t) for t in arr # type: ignore
    )

# ---