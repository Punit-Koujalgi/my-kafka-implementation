import dataclasses
import io
import os
import typing

from kafka_server.protocol.request import Request, RequestHeader
from kafka_server.protocol.response import Response, ResponseHeader
from kafka_server.metadata.cluster_metadata import ClusterMetadata
from kafka_server.protocol.protocol import *


def get_log_file_path(topic_name: str, partition_index: int) -> str:
    """Returns log file path, creating directory if needed."""
    directory = f"/tmp/kraft-combined-logs/{topic_name}-{partition_index}"
    os.makedirs(directory, exist_ok=True)
    return f"{directory}/00000000000000000000.log"


def get_next_offset(log_file_path: str) -> int:
    """Returns next available offset by reading existing log file."""
    if not os.path.exists(log_file_path):
        return 0

    try:
        with open(log_file_path, 'rb') as f:
            max_offset = -1
            while True:
                # Read base_offset (8 bytes)
                base_offset_bytes = f.read(8)
                if len(base_offset_bytes) < 8:
                    break
                base_offset = decode_int64(io.BytesIO(base_offset_bytes))

                # Read batch_length (4 bytes)
                batch_length_bytes = f.read(4)
                if len(batch_length_bytes) < 4:
                    break
                batch_length = decode_int32(io.BytesIO(batch_length_bytes))

                # Skip the batch data
                f.seek(batch_length, 1)

                # Read lastOffsetDelta from batch (at offset 20 in batch data)
                current_pos = f.tell()
                f.seek(current_pos - batch_length + 20, 0)
                last_offset_delta_bytes = f.read(4)
                if len(last_offset_delta_bytes) == 4:
                    last_offset_delta = decode_int32(io.BytesIO(last_offset_delta_bytes))
                    max_offset = max(max_offset, base_offset + last_offset_delta)

                # Move to next batch
                f.seek(current_pos, 0)

            return max_offset + 1 if max_offset >= 0 else 0
    except Exception as e:
        print(f"Error reading offsets from {log_file_path}: {e}")
        return 0


def append_record_batch_to_log(
        log_file_path: str,
        record_batch_bytes: bytes,
        new_base_offset: int
) -> tuple[int, int]:
    """Appends RecordBatch to log file with updated base_offset."""
    try:
        # Parse incoming RecordBatch
        readable = io.BytesIO(record_batch_bytes)
        _ = decode_int64(readable)  # Skip original base_offset
        batch_length = decode_int32(readable)
        batch_data = readable.read(batch_length)

        # Reconstruct with new base_offset
        updated_batch = encode_int64(new_base_offset) + encode_int32(batch_length) + batch_data

        # Append to log file
        with open(log_file_path, 'ab') as f:
            f.write(updated_batch)

        return new_base_offset, 0
    except Exception as e:
        print(f"Error appending to log {log_file_path}: {e}")
        raise


@dataclasses.dataclass(frozen=True)
class PartitionProduceData:
    index: int
    records: bytes | None

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        index = decode_int32(readable)
        print(f"[DEBUG] PartitionProduceData: index={index}")
        records = decode_compact_nullable_bytes(readable)
        print(f"[DEBUG] PartitionProduceData: records length={len(records) if records else 0}")
        data = cls(
            index=index,
            records=records,
        )
        decode_tagged_fields(readable)
        return data


@dataclasses.dataclass(frozen=True)
class TopicProduceData:
    name: str
    partition_data: list[PartitionProduceData]

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        data = cls(
            name=decode_compact_string(readable),
            partition_data=decode_compact_array(readable, PartitionProduceData.decode),
        )
        decode_tagged_fields(readable)
        return data


@dataclasses.dataclass(frozen=True)
class ProduceRequest(Request):
    transactional_id: str | None
    acks: int
    timeout_ms: int
    topic_data: list[TopicProduceData]
    is_malformed: bool = False

    @classmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> typing.Self:
        transactional_id = decode_compact_nullable_string(readable)
        acks = decode_int16(readable)
        timeout_ms = decode_int32(readable)
        topic_data = decode_compact_array(readable, TopicProduceData.decode)
        decode_tagged_fields(readable)
        return cls(
            header=header,
            transactional_id=transactional_id,
            acks=acks,
            timeout_ms=timeout_ms,
            topic_data=topic_data,
        )


@dataclasses.dataclass(frozen=True)
class PartitionProduceResponse:
    index: int
    error_code: ErrorCode
    base_offset: int
    log_append_time_ms: int
    log_start_offset: int
    record_errors: list | None = None  # Nullable compact array
    error_message: str | None = None  # Nullable compact string

    def encode(self) -> bytes:
        return b"".join(
            [
                encode_int32(self.index),
                self.error_code.encode(),
                encode_int64(self.base_offset),
                encode_int64(self.log_append_time_ms),
                encode_int64(self.log_start_offset),
                encode_compact_array(self.record_errors if self.record_errors is not None else []),  # RecordErrors
                encode_compact_nullable_string(self.error_message),  # ErrorMessage
                encode_tagged_fields(),
            ]
        )

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        data = cls(
            index=decode_int32(readable),
            error_code=ErrorCode(decode_int16(readable)),
            base_offset=decode_int64(readable),
            log_append_time_ms=decode_int64(readable),
            log_start_offset=decode_int64(readable),
            record_errors=decode_compact_array(readable, lambda r: None),  # TODO: Proper decoding
            error_message=decode_compact_nullable_string(readable),
        )
        decode_tagged_fields(readable)
        return data


@dataclasses.dataclass(frozen=True)
class TopicProduceResponse:
    name: str
    partition_responses: list[PartitionProduceResponse]

    def encode(self) -> bytes:
        return b"".join(
            [
                encode_compact_string(self.name),
                encode_compact_array(self.partition_responses),
                encode_tagged_fields(),
            ]
        )

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        data = cls(
            name=decode_compact_string(readable),
            partition_responses=decode_compact_array(readable, PartitionProduceResponse.decode),
        )
        decode_tagged_fields(readable)
        return data


@dataclasses.dataclass(frozen=True)
class ProduceResponse(Response):
    responses: list[TopicProduceResponse]
    throttle_time_ms: int

    def _encode_body(self) -> bytes:
        return b"".join(
            [
                encode_compact_array(self.responses),
                encode_int32(self.throttle_time_ms),
                encode_tagged_fields(),
            ]
        )


def handle_produce_request(request: ProduceRequest) -> ProduceResponse:
    _cluster_metadata = ClusterMetadata()
    topic_responses = []

    for topic in request.topic_data:
        partition_responses = []
        for partition in topic.partition_data:
            # Check for an invalid topic or partition
            if not _cluster_metadata.is_valid_topic(topic.name) or not _cluster_metadata.is_valid_partition(
                    topic.name, partition.index
            ):
                # --- FAILURE CASE ---
                error_code = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
                base_offset = -1
                log_start_offset = -1
            else:
                # --- SUCCESS CASE ---
                # Check if records are present
                if partition.records is None or len(partition.records) == 0:
                    # No records to persist
                    error_code = ErrorCode.NONE
                    base_offset = 0
                    log_start_offset = 0
                else:
                    # Persist records to disk
                    try:
                        log_file_path = get_log_file_path(topic.name, partition.index)
                        next_offset = get_next_offset(log_file_path)
                        base_offset, log_start_offset = append_record_batch_to_log(
                            log_file_path,
                            partition.records,
                            next_offset
                        )
                        error_code = ErrorCode.NONE
                    except Exception as e:
                        print(f"Error persisting records: {e}")
                        error_code = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
                        base_offset = -1
                        log_start_offset = -1

            # Construct the response with the correct values
            partition_response = PartitionProduceResponse(
                index=partition.index,
                error_code=error_code,
                base_offset=base_offset,
                log_append_time_ms=-1,
                log_start_offset=log_start_offset,
            )
            partition_responses.append(partition_response)

        topic_responses.append(TopicProduceResponse(name=topic.name, partition_responses=partition_responses))

    return ProduceResponse(
        header=ResponseHeader.from_request_header(request.header),
        responses=topic_responses,
        throttle_time_ms=0,
    )

