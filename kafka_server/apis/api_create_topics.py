from dataclasses import dataclass, field
from typing import Self
from uuid import UUID, uuid4
from io import BytesIO
import os

from kafka_server.metadata.cluster_metadata import ClusterMetadata
from kafka_server.metadata.record import TopicRecord, PartitionRecord, DefaultRecord, RecordHeader
from kafka_server.metadata.record_batch import MetadataRecordBatch
from kafka_server.protocol.protocol import *
from kafka_server.protocol.request import Request, RequestHeader
from kafka_server.protocol.response import Response, ResponseHeader


@dataclass(frozen=True)
class CreatableTopicConfig:
    name: str
    value: str | None

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        config = cls(
            name=decode_compact_string(readable),
            value=decode_compact_nullable_string(readable)
        )
        decode_tagged_fields(readable)
        return config

    def encode(self) -> bytes:
        return b"".join([
            encode_compact_string(self.name),
            encode_compact_nullable_string(self.value),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class CreatableTopic:
    name: str
    num_partitions: int
    replication_factor: int
    assignments: list  # ReplicaAssignment array - simplified for now
    configs: list[CreatableTopicConfig]

    @classmethod
    def decode(cls, readable: Readable) -> Self:
        topic = cls(
            name=decode_compact_string(readable),
            num_partitions=decode_int32(readable),
            replication_factor=decode_int16(readable),
            assignments=decode_compact_array(readable, lambda r: None),  # TODO: Implement ReplicaAssignment
            configs=decode_compact_array(readable, CreatableTopicConfig.decode)
        )
        decode_tagged_fields(readable)
        return topic


@dataclass(frozen=True)
class CreateTopicsRequest(Request):
    topics: list[CreatableTopic]
    timeout_ms: int
    validate_only: bool

    @classmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> Self:
        request = cls(
            header=header,
            topics=decode_compact_array(readable, CreatableTopic.decode),
            timeout_ms=decode_int32(readable),
            validate_only=decode_int8(readable) != 0
        )
        decode_tagged_fields(readable)
        return request


@dataclass(frozen=True)
class CreatableTopicConfigs:
    name: str
    value: str | None
    read_only: bool = False
    config_source: int = 0
    is_sensitive: bool = False

    def encode(self) -> bytes:
        return b"".join([
            encode_compact_string(self.name),
            encode_compact_nullable_string(self.value),
            encode_int8(self.read_only),
            encode_int8(self.config_source),
            encode_int8(self.is_sensitive),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class CreatableTopicResult:
    name: str
    topic_id: UUID
    error_code: ErrorCode
    error_message: str | None = None
    topic_config_error_code: ErrorCode = ErrorCode.NONE
    num_partitions: int = 1
    replication_factor: int = 1
    configs: list[CreatableTopicConfigs] = field(default_factory=list)

    def encode(self) -> bytes:
        return b"".join([
            encode_compact_string(self.name),
            encode_uuid(self.topic_id),
            self.error_code.encode(),
            encode_compact_nullable_string(self.error_message),
            self.topic_config_error_code.encode(),
            encode_int32(self.num_partitions),
            encode_int16(self.replication_factor),
            encode_compact_array(self.configs),
            encode_tagged_fields()
        ])


@dataclass(frozen=True)
class CreateTopicsResponse(Response):
    throttle_time_ms: int
    topics: list[CreatableTopicResult]

    def _encode_header(self) -> bytes:
        return self.header.encode(version=0)

    def _encode_body(self) -> bytes:
        return b"".join([
            encode_int32(self.throttle_time_ms),
            encode_compact_array(self.topics),
            encode_tagged_fields()
        ])


def create_topic_in_metadata(topic_name: str, topic_id: UUID, num_partitions: int) -> None:
    """Creates topic metadata records and writes them to the metadata log."""
    metadata_log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    
    # Ensure the metadata directory exists
    os.makedirs(os.path.dirname(metadata_log_path), exist_ok=True)
    
    # Get the next offset for metadata records
    next_offset = get_next_metadata_offset(metadata_log_path)
    
    # Create topic record
    topic_record_value = create_topic_record_value(topic_name, topic_id)
    topic_default_record = DefaultRecord(
        attributes=0,
        timestamp_delta=0,
        offset_delta=0,
        key=None,
        value=topic_record_value,
        headers=[]
    )
    
    # Create topic record batch
    topic_record_batch = MetadataRecordBatch(
        base_offset=next_offset,
        partition_leader_epoch=0,
        magic=2,
        crc=0,
        attributes=0,
        last_offset_delta=0,
        base_timestamp=0,
        max_timestamp=0,
        producer_id=-1,
        producer_epoch=-1,
        base_sequence=-1,
        records=[topic_default_record]
    )

    # Write topic record batch
    with open(metadata_log_path, 'ab') as f:
        f.write(topic_record_batch.encode())

    # Create partition records
    for partition_id in range(num_partitions):
        partition_record_value = create_partition_record_value(partition_id, topic_id)
        partition_default_record = DefaultRecord(
            attributes=0,
            timestamp_delta=0,
            offset_delta=partition_id + 1,
            key=None,
            value=partition_record_value,
            headers=[]
        )
        
        partition_record_batch = MetadataRecordBatch(
            base_offset=next_offset + partition_id + 1,
            partition_leader_epoch=0,
            magic=2,
            crc=0,
            attributes=0,
            last_offset_delta=0,
            base_timestamp=0,
            max_timestamp=0,
            producer_id=-1,
            producer_epoch=-1,
            base_sequence=-1,
            records=[partition_default_record]
        )

        try:
            with open(metadata_log_path, 'ab') as f:
                f.write(partition_record_batch.encode())
        except Exception as e:
            print(f"Error writing partition record batch to {metadata_log_path}: {e}")
            raise e
        # Create partition log directory and file
        partition_log_dir = f"/tmp/kraft-combined-logs/{topic_name}-{partition_id}"
        os.makedirs(partition_log_dir, exist_ok=True)
        partition_log_file = f"{partition_log_dir}/00000000000000000000.log"
        if not os.path.exists(partition_log_file):
            open(partition_log_file, 'a').close()


def get_next_metadata_offset(metadata_log_path: str) -> int:
    """Returns the next available offset in the metadata log."""
    if not os.path.exists(metadata_log_path):
        return 0
    
    try:
        with open(metadata_log_path, 'rb') as f:
            max_offset = -1
            while True:
                # Read base_offset (8 bytes)
                base_offset_bytes = f.read(8)
                if len(base_offset_bytes) < 8:
                    break
                base_offset = decode_int64(BytesIO(base_offset_bytes))
                
                # Read batch_length (4 bytes) 
                batch_length_bytes = f.read(4)
                if len(batch_length_bytes) < 4:
                    break
                batch_length = decode_int32(BytesIO(batch_length_bytes))
                
                # Skip the batch data to get to the next batch
                f.seek(batch_length, 1)
                
                # Read lastOffsetDelta from batch (at offset 20 in batch data)
                current_pos = f.tell()
                f.seek(current_pos - batch_length + 20, 0)
                last_offset_delta_bytes = f.read(4)
                if len(last_offset_delta_bytes) == 4:
                    last_offset_delta = decode_int32(BytesIO(last_offset_delta_bytes))
                    max_offset = max(max_offset, base_offset + last_offset_delta)
                
                # Move to next batch
                f.seek(current_pos, 0)
            
            return max_offset + 1 if max_offset >= 0 else 0
    except Exception as e:
        print(f"Error reading metadata offsets: {e}")
        return 0


def create_topic_record_value(topic_name: str, topic_id: UUID) -> bytes:
    """Creates the value bytes for a topic metadata record."""
    frame_version = 1  # Frame version
    record_type = 2    # TOPIC record type
    version = 0        # Record version
    
    return b"".join([
        encode_int8(frame_version),
        encode_int8(record_type), 
        encode_int8(version),
        encode_compact_string(topic_name),
        encode_uuid(topic_id),
        encode_tagged_fields()
    ])


def create_partition_record_value(partition_id: int, topic_id: UUID) -> bytes:
    """Creates the value bytes for a partition metadata record."""
    frame_version = 1  # Frame version
    record_type = 3    # PARTITION record type  
    version = 0        # Record version
    
    return b"".join([
        encode_int8(frame_version),
        encode_int8(record_type),
        encode_int8(version),
        encode_int32(partition_id),
        encode_uuid(topic_id),
        encode_compact_array([0], encode_int32),  # replicas (broker 0)
        encode_compact_array([0], encode_int32),  # isr (broker 0)
        encode_compact_array([]),   # removing_replicas
        encode_compact_array([]),   # adding_replicas  
        encode_int32(0),            # leader (broker 0)
        encode_int32(0),            # leader_epoch
        encode_int32(0),            # partition_epoch
        encode_compact_array([]),   # directories (empty UUIDs)
        encode_tagged_fields()
    ])


def handle_create_topics_request(request: CreateTopicsRequest) -> CreateTopicsResponse:
    """Handles CreateTopics API request."""
    cluster_metadata = ClusterMetadata()
    topic_results = []
    
    print(f"Received CreateTopicsRequest with {len(request.topics)} topics")

    for topic in request.topics:
        print(f"Processing topic creation: {topic.name}")
        # Validate topic name
        if not topic.name or len(topic.name.strip()) == 0:
            topic_results.append(CreatableTopicResult(
                name=topic.name,
                topic_id=UUID(int=0),
                error_code=ErrorCode.INVALID_TOPIC_EXCEPTION,
                error_message="Topic name cannot be empty"
            ))
            continue
            
        # Check if topic already exists
        if cluster_metadata.is_valid_topic(topic.name):
            topic_results.append(CreatableTopicResult(
                name=topic.name,
                topic_id=cluster_metadata.get_topic_id(topic.name) or UUID(int=0),
                error_code=ErrorCode.TOPIC_ALREADY_EXISTS,
                error_message=f"Topic '{topic.name}' already exists"
            ))
            continue
        
        # Validate partition count
        if topic.num_partitions <= 0:
            topic_results.append(CreatableTopicResult(
                name=topic.name,
                topic_id=UUID(int=0),
                error_code=ErrorCode.INVALID_TOPIC_EXCEPTION,
                error_message="Number of partitions must be positive"
            ))
            continue
            
        # If validate_only is true, don't actually create the topic
        if request.validate_only:
            topic_results.append(CreatableTopicResult(
                name=topic.name,
                topic_id=UUID(int=0),
                error_code=ErrorCode.NONE,
                num_partitions=topic.num_partitions,
                replication_factor=topic.replication_factor
            ))
            continue

        print(f"Creating topic: {topic.name} with {topic.num_partitions} partitions")
        # Create the topic
        try:
            topic_id = uuid4()
            create_topic_in_metadata(topic.name, topic_id, topic.num_partitions)
            
            # Refresh cluster metadata to include the new topic
            cluster_metadata.add_topic_direct(topic.name, topic_id)
            for partition_id in range(topic.num_partitions):
                cluster_metadata.add_partition_direct(topic_id, partition_id)
            
            topic_results.append(CreatableTopicResult(
                name=topic.name,
                topic_id=topic_id,
                error_code=ErrorCode.NONE,
                num_partitions=topic.num_partitions,
                replication_factor=topic.replication_factor
            ))
            
        except Exception as e:
            print(f"Error creating topic {topic.name}: {e})")
            topic_results.append(CreatableTopicResult(
                name=topic.name,
                topic_id=UUID(int=0),
                error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                error_message=f"Failed to create topic: {str(e)}"
            ))
            raise e
    
    return CreateTopicsResponse(
        header=ResponseHeader.from_request_header(request.header),
        throttle_time_ms=0,
        topics=topic_results
    )
