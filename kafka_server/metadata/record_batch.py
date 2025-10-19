import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from io import BytesIO
from typing import Generator, Self

from kafka_server.protocol.protocol import *
from .record import DefaultRecord, MetadataRecord, Record


@dataclass(frozen=True)
class RecordBatch(ABC):
    base_offset: int
    partition_leader_epoch: int
    magic: int
    crc: int
    attributes: int
    last_offset_delta: int
    base_timestamp: int
    max_timestamp: int
    producer_id: int
    producer_epoch: int
    base_sequence: int
    records: list[Record]

    @classmethod
    @abstractmethod
    def decode_record(cls, readable: Readable) -> Record:
        raise NotImplementedError

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        base_offset = decode_int64(readable)
        batch_length = decode_int32(readable)
        new_readable = BytesIO(readable.read(batch_length))
        return cls(
            base_offset=base_offset,
            partition_leader_epoch=decode_int32(new_readable),
            magic=decode_int8(new_readable),
            crc=decode_uint32(new_readable),
            attributes=decode_int16(new_readable),
            last_offset_delta=decode_int32(new_readable),
            base_timestamp=decode_int64(new_readable),
            max_timestamp=decode_int64(new_readable),
            producer_id=decode_int64(new_readable),
            producer_epoch=decode_int16(new_readable),
            base_sequence=decode_int32(new_readable),
            records=decode_array(new_readable, cls.decode_record),
        )

    def encode(self) -> bytes:
        data = b"".join([
            encode_int32(self.partition_leader_epoch),
            encode_int8(self.magic),
            encode_uint32(self.crc),
            encode_int16(self.attributes),
            encode_int32(self.last_offset_delta),
            encode_int64(self.base_timestamp),
            encode_int64(self.max_timestamp),
            encode_int64(self.producer_id),
            encode_int16(self.producer_epoch),
            encode_int32(self.base_sequence),
            encode_array(self.records),
        ])
        return encode_int64(self.base_offset) + encode_int32(len(data)) + data


class DefaultRecordBatch(RecordBatch):
    @classmethod
    def decode_record(cls, readable: Readable) -> Record:
        return DefaultRecord.decode(readable)


class MetadataRecordBatch(RecordBatch):
    @classmethod
    def decode_record(cls, readable: Readable) -> Record:
        return MetadataRecord.decode(readable)


def read_record_batches(topic_name: str, partition_index: int, fetch_offset: int = 0) -> Generator[RecordBatch, None, None]:
    if topic_name == "__cluster_metadata":
        assert partition_index == 0
        record_batch_class = MetadataRecordBatch
    else:
        record_batch_class = DefaultRecordBatch

    log_file_path = f"/tmp/kraft-combined-logs/{topic_name}-{partition_index}/00000000000000000000.log"
    # print(f"Fetch offset: {fetch_offset} from {log_file_path}")

    if not os.path.exists(log_file_path):
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        open(log_file_path, 'a').close()
    else:
        with open(
            log_file_path, mode="rb"
        ) as reader:
            while reader.peek():
                if fetch_offset == 0:
                    yield record_batch_class.decode(reader)
                else:
                    fetch_offset -= 1
                    record_batch_class.decode(reader)
            

