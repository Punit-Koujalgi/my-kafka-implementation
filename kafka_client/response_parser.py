

from uuid import UUID
from io import BytesIO
from dataclasses import dataclass, asdict
import json

import os
import sys
import socket
import struct

from kafka_server.protocol.protocol import *
from .request_builder import create_fetch_request_with_topic_Id

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def parse_api_versions_response_v1(data: bytes) -> dict:
    """Parse and return API_VERSIONS response."""
    readable = BytesIO(data)
    
    # Response header
    correlation_id = decode_int32(readable)
    # decode_tagged_fields(readable)  # header tagged fields
    
    # Response body
    error_code = decode_int16(readable)
    
    # API keys array
    api_keys_count = decode_unsigned_varint(readable) - 1
    api_versions = []
    
    for _ in range(api_keys_count):
        api_key = decode_int16(readable)
        min_version = decode_int16(readable)
        max_version = decode_int16(readable)
        decode_tagged_fields(readable)
        api_versions.append({
            "api_key": api_key,
            "min_version": min_version,
            "max_version": max_version
        })
    
    # Throttle time and client software name/version (for v4)
    throttle_time_ms = decode_int32(readable)
    decode_tagged_fields(readable)  # finalized_features_epoch and friends
    
    return {
        "correlation_id": correlation_id,
        "error_code": error_code,
        "api_versions": api_versions,
        "throttle_time_ms": throttle_time_ms
    }


def parse_create_topics_response_v1(data: bytes) -> list[dict]:
    """Parse and return CREATE_TOPICS response."""
    readable = BytesIO(data)
    
    # Response header
    correlation_id = decode_int32(readable)
    # decode_tagged_fields(readable)  # header tagged fields
    
    # Response body
    throttle_time_ms = decode_int32(readable)
    
    # Topics array
    topics_count = decode_unsigned_varint(readable) - 1
    results = []
    
    for _ in range(topics_count):
        name = decode_compact_string(readable)
        topic_id_bytes = readable.read(16)
        topic_id = UUID(bytes=topic_id_bytes)
        error_code = decode_int16(readable)
        error_message = decode_compact_nullable_string(readable)
        topic_config_error_code = decode_int16(readable)
        num_partitions = decode_int32(readable)
        replication_factor = decode_int16(readable)
        
        # Skip configs array
        configs_count = decode_unsigned_varint(readable) - 1
        for _ in range(configs_count):
            decode_compact_string(readable)  # name
            decode_compact_nullable_string(readable)  # value
            decode_int8(readable)  # read_only
            decode_int8(readable)  # config_source
            decode_int8(readable)  # is_sensitive
            decode_tagged_fields(readable)
        
        decode_tagged_fields(readable)
        
        results.append({
            "status": "✅" if error_code == 0 else "❌",
            "topic_name": name,
            "topic_id": str(topic_id),
            "partitions": num_partitions,
            "replication_factor": replication_factor,
            "error_code": error_code,
            "error_message": error_message
        })
    
    return results



def parse_metadata_response_v1(data: bytes) -> dict:
    """Parse and return METADATA response."""
    readable = BytesIO(data)
    
    # Response header
    correlation_id = decode_int32(readable)
    decode_tagged_fields(readable)  # header tagged fields
    
    # Response body
    throttle_time_ms = decode_int32(readable)
    
    # Brokers array
    brokers_count = decode_unsigned_varint(readable) - 1
    brokers = []
    for _ in range(brokers_count):
        node_id = decode_int32(readable)
        host = decode_compact_string(readable)
        port = decode_int32(readable)
        rack = decode_compact_nullable_string(readable)
        decode_tagged_fields(readable)
        brokers.append({
            "node_id": node_id,
            "host": host,
            "port": port,
            "rack": rack
        })
    
    # Cluster info
    cluster_id = decode_compact_nullable_string(readable)
    controller_id = decode_int32(readable)
    
    # Topics array
    topics_count = decode_unsigned_varint(readable) - 1
    topics = []
    total_partitions = 0
    
    for _ in range(topics_count):
        error_code = decode_int16(readable)
        topic_name = decode_compact_nullable_string(readable)
        topic_id_bytes = readable.read(16)
        topic_id = UUID(bytes=topic_id_bytes)
        is_internal = decode_int8(readable) != 0
        
        # Partitions array
        partitions_count = decode_unsigned_varint(readable) - 1
        partitions = []
        total_partitions += partitions_count
        
        for _ in range(partitions_count):
            part_error_code = decode_int16(readable)
            partition_index = decode_int32(readable)
            leader_id = decode_int32(readable)
            leader_epoch = decode_int32(readable)
            
            # Replica nodes
            replica_count = decode_unsigned_varint(readable) - 1
            replica_nodes = []
            for _ in range(replica_count):
                replica_nodes.append(decode_int32(readable))
            
            # ISR nodes
            isr_count = decode_unsigned_varint(readable) - 1
            isr_nodes = []
            for _ in range(isr_count):
                isr_nodes.append(decode_int32(readable))
            
            # Offline replicas
            offline_count = decode_unsigned_varint(readable) - 1
            offline_replicas = []
            for _ in range(offline_count):
                offline_replicas.append(decode_int32(readable))
            
            decode_tagged_fields(readable)
            
            partitions.append({
                "partition_index": partition_index,
                "leader_id": leader_id,
                "replica_nodes": replica_nodes,
                "isr_nodes": isr_nodes,
                "error_code": part_error_code
            })
        
        topic_auth_ops = decode_int32(readable)
        decode_tagged_fields(readable)
        
        topics.append({
            "topic_name": topic_name,
            "topic_id": topic_id,
            "partitions": partitions,
            "is_internal": is_internal,
            "error_code": error_code
        })
    
    cluster_auth_ops = decode_int32(readable)
    decode_tagged_fields(readable)
    
    return {
        "total_topics": topics_count,
        "total_partitions": total_partitions,
        "total_records": 0,  # This would need to be calculated separately
        "topics": [{
            "topic_name": topic["topic_name"],
            "partitions": len(topic["partitions"]),
            "topic_id": str(topic["topic_id"])
        } for topic in topics],
        "brokers": brokers,
        "cluster_id": cluster_id,
        "controller_id": controller_id
    }


def parse_fetch_response_v1(data: bytes) -> list[dict]:
    """Parse and return FETCH response."""
    readable = BytesIO(data)

    # Response header
    correlation_id = decode_int32(readable)
    decode_tagged_fields(readable)  # header tagged fields

    # Response body
    throttle_time_ms = decode_int32(readable)
    error_code = decode_int16(readable)
    session_id = decode_int32(readable)

    # Responses array
    responses_count = decode_unsigned_varint(readable) - 1
    results = []

    for _ in range(responses_count):
        topic_id = decode_uuid(readable)

        partitions_count = decode_unsigned_varint(readable) - 1

        for _ in range(partitions_count):
            partition_index = decode_int32(readable)
            error_code = decode_int16(readable)
            high_watermark = decode_int64(readable)
            last_stable_offset = decode_int64(readable)
            log_start_offset = decode_int64(readable)

            # Skip aborted transactions array
            decode_compact_array(readable, lambda r: None)

            preferred_read_replica = decode_int32(readable)

            # Records
            records_length = decode_unsigned_varint(readable)
            records_data = readable.read(records_length) if records_length > 0 else b""
            
            # Parse records
            messages = []
            if records_data:
                try:
                    from kafka_server.metadata.record_batch import DefaultRecordBatch
                    from kafka_server.metadata.record import DefaultRecord
                    records = BytesIO(records_data)
                    record_batch = DefaultRecordBatch.decode(records)
                    for record in record_batch.records:
                        if isinstance(record, DefaultRecord):
                            messages.append({
                                "key": record.key.decode() if record.key else "",
                                "value": record.value.decode() if record.value else "",
                                "offset": record_batch.base_offset + record.offset_delta
                            })
                except Exception as e:
                    # If parsing fails, just include raw data
                    pass

            decode_tagged_fields(readable)
            
            results.append({
                "topic_id": str(topic_id),
                "partition": partition_index,
                "messages": messages,
                "high_watermark": high_watermark,
                "error_code": error_code
            })

    decode_tagged_fields(readable)
    return results

def parse_produce_response_v1(data: bytes) -> list[dict]:
    """Parse and return PRODUCE response."""
    readable = BytesIO(data)

    # Response header
    correlation_id = decode_int32(readable)
    decode_tagged_fields(readable)  # header tagged fields

    # Response body
    responses_count = decode_unsigned_varint(readable) - 1
    results = []

    for _ in range(responses_count):
        topic_name = decode_compact_string(readable)

        partitions_count = decode_unsigned_varint(readable) - 1

        for _ in range(partitions_count):
            partition_index = decode_int32(readable)
            error_code = decode_int16(readable)
            base_offset = decode_int64(readable)
            log_append_time_ms = decode_int64(readable)
            log_start_offset = decode_int64(readable)

            # Skip record errors array
            decode_compact_array(readable, lambda r: None)

            error_message = decode_compact_nullable_string(readable)

            decode_tagged_fields(readable)
            
            results.append({
                "topic": topic_name,
                "partition": partition_index,
                "records_added": 1 if error_code == 0 else 0,  # Simplified
                "base_offset": base_offset,
                "error_code": error_code,
                "error_message": error_message
            })

    decode_tagged_fields(readable)
    return results


def parse_describe_topics_response_v1(data: bytes) -> list[dict]:
    """Parse and return DESCRIBE_TOPIC_PARTITIONS response."""
    readable = BytesIO(data)

    # Response header
    correlation_id = decode_int32(readable)
    decode_tagged_fields(readable)  # header tagged fields

    # Response body
    throttle_time_ms = decode_int32(readable)
    
    # Topics array
    topics_count = decode_unsigned_varint(readable) - 1
    results = []

    for _ in range(topics_count):
        error_code = decode_int16(readable)
        topic_name = decode_compact_nullable_string(readable)
        topic_id_bytes = readable.read(16)
        topic_id = UUID(bytes=topic_id_bytes)
        is_internal = decode_int8(readable) != 0
        
        # Partitions array
        partitions_count = decode_unsigned_varint(readable) - 1
        
        for _ in range(partitions_count):
            partition_error_code = decode_int16(readable)
            partition_index = decode_int32(readable)
            leader_id = decode_int32(readable)
            leader_epoch = decode_int32(readable)
            
            # Replica nodes array
            replica_count = decode_unsigned_varint(readable) - 1
            replica_nodes = []
            for _ in range(replica_count):
                replica_nodes.append(decode_int32(readable))
            
            # ISR nodes array
            isr_count = decode_unsigned_varint(readable) - 1
            isr_nodes = []
            for _ in range(isr_count):
                isr_nodes.append(decode_int32(readable))
            
            # Eligible leader replicas array
            eligible_count = decode_unsigned_varint(readable) - 1
            for _ in range(eligible_count):
                decode_int32(readable)
            
            # Last known ELR array
            last_known_count = decode_unsigned_varint(readable) - 1
            for _ in range(last_known_count):
                decode_int32(readable)
            
            # Offline replicas array
            offline_count = decode_unsigned_varint(readable) - 1
            for _ in range(offline_count):
                decode_int32(readable)
            
            decode_tagged_fields(readable)
            
            results.append({
                "status": "✅" if partition_error_code == 0 else "❌",
                "topic_name": topic_name,
                "partition": partition_index,
                "error_code": partition_error_code,
                "leader_id": leader_id,
                "replicas": len(replica_nodes)
            })
        
        topic_auth_ops = decode_int32(readable)
        decode_tagged_fields(readable)
    
    # Next cursor
    decode_int8(readable)  # cursor (null byte)
    decode_tagged_fields(readable)
    
    return results


# V2 Functions - Direct server integration for UI
import time
import uuid
from kafka_server.protocol.response import handle_request
from kafka_server.protocol.request import Request
from kafka_server.apis.api_versions import ApiVersionsRequest
from kafka_server.apis.api_create_topics import CreateTopicsRequest, CreateTopicsResponse
from kafka_server.apis.api_describe_topic_partitions import DescribeTopicPartitionsRequest, DescribePartitionsResponse
from kafka_server.apis.api_metadata import MetadataRequest, MetadataResponse
from kafka_server.apis.api_fetch import FetchRequest, FetchResponse
from kafka_server.apis.api_produce import ProduceRequest, ProduceResponse


def read_response(request: Request) -> dict:
    """Process request directly using server and return structured response for UI."""
    response = handle_request(request)
    return {
        "request": str(request),
        "response": str(response),
        "timestamp": time.time()
    }


def parse_api_versions_response(request: ApiVersionsRequest, add_api_log: Callable) -> tuple:
    """Handle API_VERSIONS request and return UI-friendly response."""
    response = handle_request(request)
    dropDown = add_api_log("ApiVersionsResponse", response)
    return response, dropDown


def parse_create_topics_response(request: CreateTopicsRequest, add_api_log: Callable) -> tuple:
    """Handle CREATE_TOPICS request and return UI-friendly response."""
    response: CreateTopicsResponse = handle_request(request) # type: ignore
    dropDown = add_api_log("CreateTopicsResponse", response)

    results = []
    for topic in response.topics:
        results.append({
            "status": "✅",
            "topic_name": topic.name,
            "topic_id": str(topic.topic_id),
            "partitions": topic.num_partitions,
            "replication_factor": topic.replication_factor
        })

    return results, dropDown


def parse_describe_topics_response(request: DescribeTopicPartitionsRequest, add_api_log: Callable, topic_requests) -> tuple:
    """Handle DESCRIBE_TOPIC_PARTITIONS request and return UI-friendly response."""
    response: DescribePartitionsResponse = handle_request(request) # type: ignore
    dropDown = add_api_log("DescribeTopicPartitionsResponse", response)

    results = []
    for topic in response.topics:
        for partition in topic.partitions:

            current_topic_request = [topic_request for topic_request in topic_requests if topic_request["topic_name"] == topic.name][0]
            if partition.partition_index not in current_topic_request["partitions"]:
                continue

            results.append({
                "status": "✅ Healthy",
                "topic_name": topic.name,
                "partition": partition.partition_index,
                "error_code": 0,
                "leader_id": partition.leader_id,
                "replicas": partition.replica_nodes
            })

    return results, dropDown


def parse_metadata_response(request: MetadataRequest, add_api_log: Callable | None = None) -> tuple:
    """Handle METADATA request and return UI-friendly response."""
    response: MetadataResponse = handle_request(request) # type: ignore
    dropDown = None
    if add_api_log:
        dropDown = add_api_log("MetadataResponse", response)

    topics_data = []
    total_partitions = 0
    total_records = 0
    
    for topic in response.topics:
        for partition in topic.partitions:
            total_partitions += 1
            partition_records = 0

            # Send a fetch request to get record count for this partition
            fetch_request = create_fetch_request_with_topic_Id(topic.topic_id, partition.partition_index)
            fetch_response: FetchResponse = handle_request(fetch_request)  # type: ignore
            if fetch_response.responses and fetch_response.responses[0].partitions:
                for record_batch in fetch_response.responses[0].partitions[0].records:
                    partition_records += len(record_batch.records)

            topic_data = {
                "topic_name": topic.name,
                "topic_id": str(topic.topic_id),
                "partitions": partition.partition_index,
                "records": partition_records
            }

            topics_data.append(topic_data)
            total_records += partition_records

    return {
        "topics": topics_data,
        "total_topics": len(response.topics),
        "total_partitions": total_partitions,
        "total_records": total_records
    }, dropDown

def parse_fetch_response(request: FetchRequest, add_api_log: Callable) -> tuple:
    """Handle FETCH request and return UI-friendly response."""
    
    from .api import convert_uuid_to_topic_name

    response: FetchResponse = handle_request(request) # type: ignore
    dropDown = add_api_log("FetchResponse", response)

    results = []
    for fetch_response in response.responses:
        # Convert topic UUID to name
        topic_name = convert_uuid_to_topic_name(fetch_response.topic_id)
        for partition_data in fetch_response.partitions:
            for record_batch in partition_data.records:
                # print("----- Batch ----- ")
                for record in record_batch.records:
                    # print(f"Record: Key={record.key.decode()}, Value={record.value.decode()}")
                    results.append({
                        "Topic": topic_name,
                        "Partition": partition_data.partition_index,
                        "Batch offset": record_batch.base_offset,
                        "Key": record.key.decode(), "Value": record.value.decode() # type: ignore
                    })
    
    return results, dropDown


def parse_produce_response(request: ProduceRequest, add_api_log: Callable, produce_request_ui) -> tuple:
    """Handle PRODUCE request and return UI-friendly response."""
    response: ProduceResponse = handle_request(request) # type: ignore
    dropDown = add_api_log("ProduceResponse", response)

    results = []
    for topic_response in response.responses:
        for partition_data in topic_response.partition_responses:
            
            current_request = [topic for topic in produce_request_ui if topic["topic_name"] == topic_response.name][0]

            results.append({
                "topic": topic_response.name,
                "partition": partition_data.index,
                "records_added": len(current_request["records"]) if current_request else 0,
                "base_offset": partition_data.base_offset,
            })
    
    return results, dropDown


