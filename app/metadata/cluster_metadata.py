from collections import defaultdict
from typing import Self
from uuid import UUID

from .record import PartitionRecord, TopicRecord
from .record_batch import read_record_batches


class ClusterMetadata:
    _instance = None

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._name_to_id: dict[str, UUID] = {}
        self._id_to_name: dict[UUID, str] = {}
        self._id_to_partitions = defaultdict[UUID, list[int]](list)

        for record_batch in read_record_batches("__cluster_metadata", 0):
            for record in record_batch.records:
                if isinstance(record, PartitionRecord):
                    self._add_partition_record(record)
                elif isinstance(record, TopicRecord):
                    self._add_topic_record(record)

    def get_topic_name(self, topic_id: UUID) -> str | None:
        return self._id_to_name.get(topic_id)

    def get_topic_id(self, topic_name: str) -> UUID | None:
        return self._name_to_id.get(topic_name)

    def get_topic_partitions(self, topic_id: UUID) -> list[int]:
        return self._id_to_partitions.get(topic_id, list())

    def _add_partition_record(self, record: PartitionRecord) -> None:
        self._id_to_partitions[record.topic_id].append(record.partition_id)

    def _add_topic_record(self, record: TopicRecord) -> None:
        self._name_to_id[record.name] = record.topic_id
        self._id_to_name[record.topic_id] = record.name
