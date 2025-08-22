import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BytesIO
from typing import Self
from uuid import UUID

from app.protocol.protocol import *


class Record(ABC):
    @classmethod
    @abstractmethod
    def decode(cls, readable: Readable) -> Self:
        raise NotImplementedError

    @abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError


@dataclass(frozen=True)
class RecordHeader:
    key: str
    value: bytes

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        key_length = decode_varint(readable)
        key = readable.read(key_length).decode()
        value_length = decode_varint(readable)
        value = readable.read(value_length)
        return cls(key, value)

    def encode(self) -> bytes:
        return b"".join([
            encode_varint(len(self.key)),
            self.key.encode(),
            encode_varint(len(self.value)),
            self.value,
        ])


@dataclass(frozen=True)
class DefaultRecord(Record):
    attributes: int
    timestamp_delta: int
    offset_delta: int
    key: bytes | None
    value: bytes
    headers: list[RecordHeader]

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        length = decode_varint(readable)
        new_readable = BytesIO(readable.read(length))
        attributes = decode_int8(new_readable)
        timestamp_delta = decode_varlong(new_readable)
        offset_delta = decode_varint(new_readable)
        key_length = decode_varint(new_readable)
        key = None if key_length < 0 else new_readable.read(key_length)
        value_length = decode_varint(new_readable)
        value = new_readable.read(value_length)
        headers_count = decode_unsigned_varint(new_readable)
        headers = [RecordHeader.decode(new_readable) for _ in range(headers_count)]
        return cls(
            attributes=attributes,
            timestamp_delta=timestamp_delta,
            offset_delta=offset_delta,
            key=key,
            value=value,
            headers=headers,
        )

    def encode(self) -> bytes:
        data = b"".join([
            encode_int8(self.attributes),
            encode_varlong(self.timestamp_delta),
            encode_varint(self.offset_delta),
            encode_varint(-1) if self.key is None else encode_varint(len(self.key)) + self.key,
            encode_varint(len(self.value)),
            self.value,
            encode_unsigned_varint(len(self.headers)),
            b"".join(header.encode() for header in self.headers),
        ])
        return encode_varint(len(data)) + data


@enum.unique
class MetadataRecordType(enum.IntEnum):
    TOPIC = 2
    PARTITION = 3
    FEATURE_LEVEL = 12

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        return cls(decode_int8(readable))


@dataclass(frozen=True)
class MetadataRecord(Record):
    record: DefaultRecord

    @classmethod
    @abstractmethod
    def decode_value(cls, record: DefaultRecord, readable: Readable) -> Self:
        raise NotImplementedError

    @classmethod
    def decode(cls, readable: Readable) -> "TopicRecord | PartitionRecord | FeatureLevelRecord":
        record = DefaultRecord.decode(readable)
        new_readable = BytesIO(record.value)

        frame_version = decode_int8(new_readable)
        record_type = MetadataRecordType.decode(new_readable)
        version = decode_int8(new_readable)

        match record_type:
            case MetadataRecordType.TOPIC:
                return TopicRecord.decode_value(record, new_readable)
            case MetadataRecordType.PARTITION:
                return PartitionRecord.decode_value(record, new_readable)
            case MetadataRecordType.FEATURE_LEVEL:
                return FeatureLevelRecord.decode_value(record, new_readable)

    def encode(self) -> bytes:
        return self.record.encode()


@dataclass(frozen=True)
class TopicRecord(MetadataRecord):
    name: str
    topic_id: UUID

    @classmethod
    def decode_value(cls, record: DefaultRecord, readable: Readable) -> Self:
        topic_record = cls(
            record=record,
            name=decode_compact_string(readable),
            topic_id=decode_uuid(readable),
        )
        decode_tagged_fields(readable)
        return topic_record


@dataclass(frozen=True)
class PartitionRecord(MetadataRecord):
    partition_id: int
    topic_id: UUID
    replicas: list[int]
    isr: list[int]
    removing_replicas: list[int]
    adding_replicas: list[int]
    leader: int
    leader_epoch: int
    partition_epoch: int
    directories: list[UUID]

    @classmethod
    def decode_value(cls, record: DefaultRecord, readable: Readable) -> Self:
        partition_record = cls(
            record=record,
            partition_id=decode_int32(readable),
            topic_id=decode_uuid(readable),
            replicas=decode_compact_array(readable, decode_int32),
            isr=decode_compact_array(readable, decode_int32),
            removing_replicas=decode_compact_array(readable, decode_int32),
            adding_replicas=decode_compact_array(readable, decode_int32),
            leader=decode_int32(readable),
            leader_epoch=decode_int32(readable),
            partition_epoch=decode_int32(readable),
            directories=decode_compact_array(readable, decode_uuid),
        )
        decode_tagged_fields(readable)
        return partition_record


@dataclass(frozen=True)
class FeatureLevelRecord(MetadataRecord):
    name: str
    feature_level: int

    @classmethod
    def decode_value(cls, record: DefaultRecord, readable: Readable) -> Self:
        feature_level_record = cls(
            record=record,
            name=decode_compact_string(readable),
            feature_level=decode_int16(readable),
        )
        decode_tagged_fields(readable)
        return feature_level_record
