from dataclasses import dataclass, field
from typing import Self
from uuid import UUID

from kafka_server.metadata.cluster_metadata import ClusterMetadata
from kafka_server.protocol.protocol import *
from kafka_server.protocol.request import Request, RequestHeader
from kafka_server.protocol.response import Response, ResponseHeader


@dataclass(frozen=True)
class MetadataRequestTopic:
    topic_id: UUID
    name: str | None

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        topic = cls(
            topic_id=decode_uuid(readable),
            name=decode_compact_nullable_string(readable)
        )
        decode_tagged_fields(readable)
        return topic


@dataclass(frozen=True)
class MetadataRequest(Request):
    topics: list[MetadataRequestTopic] | None
    allow_auto_topic_creation: bool
    include_cluster_authorized_operations: bool
    include_topic_authorized_operations: bool

    @classmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> Self:
        # Decode topics array - can be null
        topics_array_length = decode_unsigned_varint(readable)
        if topics_array_length == 0:
            topics = None
        else:
            topics = [MetadataRequestTopic.decode(readable) for _ in range(topics_array_length - 1)]
        
        request = cls(
            header=header,
            topics=topics,
            allow_auto_topic_creation=decode_int8(readable) != 0,
            include_cluster_authorized_operations=decode_int8(readable) != 0,
            include_topic_authorized_operations=decode_int8(readable) != 0
        )
        decode_tagged_fields(readable)
        return request


@dataclass(frozen=True)
class MetadataResponseBroker:
    node_id: int
    host: str
    port: int
    rack: str | None = None

    def encode(self) -> bytes:
        return b"".join([
            encode_int32(self.node_id),
            encode_compact_string(self.host),
            encode_int32(self.port),
            encode_compact_nullable_string(self.rack),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class MetadataResponsePartition:
    error_code: ErrorCode
    partition_index: int
    leader_id: int
    leader_epoch: int
    replica_nodes: list[int]
    isr_nodes: list[int]
    offline_replicas: list[int] = field(default_factory=list)

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_int32(self.partition_index),
            encode_int32(self.leader_id),
            encode_int32(self.leader_epoch),
            encode_compact_array(self.replica_nodes, encode_int32),
            encode_compact_array(self.isr_nodes, encode_int32),
            encode_compact_array(self.offline_replicas, encode_int32),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class MetadataResponseTopic:
    error_code: ErrorCode
    name: str | None
    topic_id: UUID
    is_internal: bool
    partitions: list[MetadataResponsePartition]
    topic_authorized_operations: int = -2147483648  # Default value

    def encode(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_compact_nullable_string(self.name),
            encode_uuid(self.topic_id),
            encode_int8(self.is_internal),
            encode_compact_array(self.partitions),
            encode_int32(self.topic_authorized_operations),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class MetadataResponse(Response):
    throttle_time_ms: int
    brokers: list[MetadataResponseBroker]
    cluster_id: str | None
    controller_id: int
    topics: list[MetadataResponseTopic]
    cluster_authorized_operations: int = -2147483648  # Default value

    def _encode_body(self) -> bytes:
        return b"".join([
            encode_int32(self.throttle_time_ms),
            encode_compact_array(self.brokers),
            encode_compact_nullable_string(self.cluster_id),
            encode_int32(self.controller_id),
            encode_compact_array(self.topics),
            encode_int32(self.cluster_authorized_operations),
            encode_tagged_fields()
        ])


def _create_topic_partitions(cluster_metadata: ClusterMetadata, topic_id: UUID) -> list[MetadataResponsePartition]:
    """Create partition metadata for a given topic."""
    partitions = []
    for partition_index in cluster_metadata.get_topic_partitions(topic_id):
        partitions.append(MetadataResponsePartition(
            error_code=ErrorCode.NONE,
            partition_index=partition_index,
            leader_id=0,  # Single broker with ID 0
            leader_epoch=0,
            replica_nodes=[0],  # Single replica on broker 0
            isr_nodes=[0]  # Single ISR on broker 0
        ))
    return partitions


def handle_metadata_request(request: MetadataRequest) -> MetadataResponse:
    """Handle METADATA API request."""
    cluster_metadata = ClusterMetadata()
    
    # Create broker list (we'll use a single broker for simplicity)
    brokers = [
        MetadataResponseBroker(
            node_id=0,
            host="localhost",
            port=9092,
            rack=None
        )
    ]
    
    # Handle topic metadata
    topics = []
    
    if request.topics is None:
        # Return metadata for all topics
        print("Returning Metadata for all topics")
        for topic_name, topic_id in cluster_metadata.get_all_topics():
            # Get partition metadata for each topic
            partitions = _create_topic_partitions(cluster_metadata, topic_id)
            
            topics.append(MetadataResponseTopic(
                error_code=ErrorCode.NONE,
                name=topic_name,
                topic_id=topic_id,
                is_internal=False,
                partitions=partitions
            ))
    else:
        # Return metadata for specific topics
        for topic_request in request.topics:
            if topic_request.name is not None:
                topic_name = topic_request.name
                topic_id = cluster_metadata.get_topic_id(topic_name)
                
                if topic_id is None:
                    # Topic doesn't exist
                    topics.append(MetadataResponseTopic(
                        error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        name=topic_name,
                        topic_id=UUID(int=0),
                        is_internal=False,
                        partitions=[]
                    ))
                else:
                    # Topic exists - get partition metadata
                    partitions = _create_topic_partitions(cluster_metadata, topic_id)
                    
                    topics.append(MetadataResponseTopic(
                        error_code=ErrorCode.NONE,
                        name=topic_name,
                        topic_id=topic_id,
                        is_internal=False,
                        partitions=partitions
                    ))
    
    return MetadataResponse(
        header=ResponseHeader.from_request_header(request.header),
        throttle_time_ms=0,
        brokers=brokers,
        cluster_id="punit-kafka-cluster",
        controller_id=0,
        topics=topics
    )