from dataclasses import dataclass, field
from typing import Self
from uuid import UUID

from kafka_server.metadata.cluster_metadata import ClusterMetadata, read_record_batches
from kafka_server.metadata.record_batch import RecordBatch
from kafka_server.protocol.protocol import *
from kafka_server.protocol.request import Request, RequestHeader
from kafka_server.protocol.response import Response, ResponseHeader


@dataclass(frozen=True)
class FetchPartition:
	partition: int
	current_leader_epoch: int
	fetch_offset: int
	last_fetched_epoch: int
	log_start_offset: int
	partition_max_bytes: int

	@classmethod
	def decode(cls, readable: Readable) -> Self:
		fetch_partition = cls(
			partition=decode_int32(readable),
			current_leader_epoch=decode_int32(readable),
			fetch_offset=decode_int64(readable),
			last_fetched_epoch=decode_int32(readable),
			log_start_offset=decode_int64(readable),
			partition_max_bytes=decode_int32(readable),
		)
		decode_tagged_fields(readable)
		return fetch_partition


@dataclass(frozen=True)
class FetchTopic:
	topic_id: UUID
	partitions: list[FetchPartition]

	@classmethod
	def decode(cls, readable: Readable) -> Self:
		fetch_topic = cls(
			topic_id=decode_uuid(readable),
			partitions=decode_compact_array(readable, FetchPartition.decode),
		)
		decode_tagged_fields(readable)
		return fetch_topic


@dataclass(frozen=True)
class ForgottenTopic:
	topic_id: UUID
	partitions: list[int]

	@classmethod
	def decode(cls, readable: Readable) -> Self:
		forgotten_topic = cls(
			topic_id=decode_uuid(readable),
			partitions=decode_compact_array(readable, decode_int32),
		)
		decode_tagged_fields(readable)
		return forgotten_topic


@dataclass(frozen=True)
class FetchRequest(Request):
	max_wait_ms: int
	min_bytes: int
	max_bytes: int
	isolation_level: int
	session_id: int
	session_epoch: int
	topics: list[FetchTopic]
	forgotten_topics_data: list
	rack_id: str

	@classmethod
	def decode_body(cls, header: RequestHeader, readable: Readable) -> Self:
		request = cls(
			header=header,
			max_wait_ms=decode_int32(readable),
			min_bytes=decode_int32(readable),
			max_bytes=decode_int32(readable),
			isolation_level=decode_int8(readable),
			session_id=decode_int32(readable),
			session_epoch=decode_int32(readable),
			topics=decode_compact_array(readable, FetchTopic.decode),
			forgotten_topics_data=decode_compact_array(readable, ForgottenTopic.decode),
			rack_id=decode_compact_string(readable),
		)
		decode_tagged_fields(readable)
		return request


@dataclass(frozen=True)
class AbortedTransaction:
	producer_id: int
	first_offset: int

	def encode(self) -> bytes:
		return b"".join([
			encode_int64(self.producer_id),
			encode_int64(self.first_offset),
			encode_tagged_fields(),
		])


@dataclass(frozen=True)
class PartitionData:
	partition_index: int
	error_code: ErrorCode
	high_watermark: int = 0
	last_stable_offset: int = 0
	log_start_offset: int = 0
	aborted_transactions: list[AbortedTransaction] = field(default_factory=list)
	preferred_read_replica: int = 0
	records: list[RecordBatch] = field(default_factory=list) # Always has 1 element

	def encode(self) -> bytes:
		encoded_records = b"".join(record.encode() for record in self.records)
		return b"".join([
			encode_int32(self.partition_index),
			self.error_code.encode(),
			encode_int64(self.high_watermark),
			encode_int64(self.last_stable_offset),
			encode_int64(self.log_start_offset),
			encode_compact_array(self.aborted_transactions),
			encode_int32(self.preferred_read_replica),
			encode_unsigned_varint(len(encoded_records)),
			encoded_records,
			encode_tagged_fields(),
		])


@dataclass(frozen=True)
class FetchableTopicResponse:
	topic_id: UUID
	partitions: list[PartitionData]

	def encode(self) -> bytes:
		return b"".join([
			encode_uuid(self.topic_id),
			encode_compact_array(self.partitions),
			encode_tagged_fields(),
		])


@dataclass(frozen=True)
class FetchResponse(Response):
	throttle_time_ms: int
	error_code: ErrorCode
	session_id: int
	responses: list[FetchableTopicResponse]

	def _encode_body(self) -> bytes:
		return b"".join([
			encode_int32(self.throttle_time_ms),
			self.error_code.encode(),
			encode_int32(self.session_id),
			encode_compact_array(self.responses),
			encode_tagged_fields(),
		])


def handle_fetch_request(request: FetchRequest) -> FetchResponse:
	return FetchResponse(
		header=ResponseHeader.from_request_header(request.header),
		throttle_time_ms=0,
		error_code=ErrorCode.NONE,
		session_id=0,
		responses=[_handle_fetch_topic(fetch_topic) for fetch_topic in request.topics],
	)


def _handle_fetch_topic(fetch_topic: FetchTopic) -> FetchableTopicResponse:
	cluster_metadata = ClusterMetadata()

	if (topic_name := cluster_metadata.get_topic_name(fetch_topic.topic_id)) is None:
		return FetchableTopicResponse(
			topic_id=fetch_topic.topic_id,
			partitions=[
				PartitionData(partition_index=0, error_code=ErrorCode.UNKNOWN_TOPIC_ID),
			],
		)


	return FetchableTopicResponse(
		topic_id=fetch_topic.topic_id,
		partitions=[
			PartitionData(
				partition_index=p.partition,
				error_code=ErrorCode.NONE,
				records=(temp_records := list(read_record_batches(topic_name, p.partition, p.fetch_offset))),
				last_stable_offset=(temp_records[-1].base_offset if temp_records else 0)
			)
			for p in fetch_topic.partitions
		],
	)


