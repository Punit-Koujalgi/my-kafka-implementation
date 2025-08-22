

from dataclasses import dataclass, field
from typing import Self
from uuid import UUID

from app.metadata.cluster_metadata import ClusterMetadata
from app.protocol.protocol import *
from app.protocol.request import Request, RequestHeader
from app.protocol.response import Response, ResponseHeader

    
@dataclass(frozen=True)
class Cursor:
    topic_name: str
    partition_index: int

    @classmethod
    def decode(cls, readable: Readable) -> Self | None:
        if (ord(readable.read(1)) & 0x80):
            return None
        cursor = cls(
            topic_name=decode_compact_string(readable),
            partition_index=decode_int32(readable)
        )
        decode_tagged_fields(readable)
        return cursor
    
# Not adding below class inside class because first parameter can be None as well
def encode_cursor(cursor: Cursor | None) -> bytes:
    if cursor is None:
        return b"\xff"
    
    return b"\x01" + b"".join([
        encode_compact_string(cursor.topic_name),
        encode_int32(cursor.partition_index),
        encode_tagged_fields()
    ])

    

@dataclass(frozen=True)
class RequestTopic:
    name: str

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        topic_request = cls(name=decode_compact_string(readable))
        decode_tagged_fields(readable)
        return topic_request

@dataclass(frozen=True)
class DescribeTopicPartitionsRequest(Request):
    topics: list[RequestTopic]
    response_partition_limit: int
    cursor: Cursor | None

    @classmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> Self:
        request = cls(
            header=header,
            topics=decode_compact_array(readable, RequestTopic.decode),
            response_partition_limit=decode_int32(readable),
            cursor=Cursor.decode(readable)
        )
        decode_tagged_fields(readable)
        return request


@dataclass(frozen=True)
class ResponsePartition:
    error_code: ErrorCode
    partition_index: int
    leader_id: int = 0
    leader_epoch: int = 0
    replica_nodes: list[int] = field(default_factory=list)
    isr_nodes: list[int] = field(default_factory=list)
    eligible_leader_replicas: list[int] = field(default_factory=list)
    last_known_elr: list[int] = field(default_factory=list)
    offline_replicas: list[int] = field(default_factory=list)

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_int32(self.partition_index),
            encode_int32(self.leader_id),
            encode_int32(self.leader_epoch),
            encode_compact_array(self.replica_nodes, encode_int32),
            encode_compact_array(self.isr_nodes, encode_int32),
            encode_compact_array(self.eligible_leader_replicas, encode_int32),
            encode_compact_array(self.last_known_elr, encode_int32),
            encode_compact_array(self.offline_replicas, encode_int32),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class ResponseTopic:
    error_code: ErrorCode
    name: str
    topic_id: UUID
    is_internal: bool = False
    partitions: list[ResponsePartition] = field(default_factory=list)
    topic_authorized_operations: int = 0

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_compact_string(self.name),
            encode_uuid(self.topic_id),
            encode_int8(self.is_internal),
            encode_compact_array(self.partitions, ResponsePartition.encode),
            encode_int32(self.topic_authorized_operations),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class DescribePartitionsResponse(Response):
    throttle_time_ms: int
    topics: list[ResponseTopic]
    cursor: Cursor | None = None

    def _encode_body(self) -> bytes:
        return b"".join([
            encode_int32(self.throttle_time_ms),
            encode_compact_array(self.topics, ResponseTopic.encode),
            encode_cursor(self.cursor),
            encode_tagged_fields()
        ])


def handle_describe_topic_partitions_request(request: DescribeTopicPartitionsRequest) -> DescribePartitionsResponse:
    return DescribePartitionsResponse(
        header=ResponseHeader.from_request_header(request.header),
        throttle_time_ms=0,
        topics=[],
    )

def _handle_topic_request(topic_request: RequestTopic) -> ResponseTopic:
    cluster_metadata = ClusterMetadata()

    if (topic_id := cluster_metadata.get_topic_id(topic_request.name)) is None:
        return ResponseTopic(
            error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
            name=topic_request.name,
            topic_id=UUID(int=0),
        )

    return ResponseTopic(
        error_code=ErrorCode.NONE,
        name=topic_request.name,
        topic_id=topic_id,
        partitions=[
            ResponsePartition(error_code=ErrorCode.NONE, partition_index=partition_index)
            for partition_index in cluster_metadata.get_topic_partitions(topic_id)
        ],
    )


    