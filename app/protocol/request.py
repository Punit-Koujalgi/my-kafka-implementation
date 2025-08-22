
from dataclasses import dataclass
from typing import Self, NoReturn
from abc import ABC, abstractmethod

from .protocol import *

@dataclass
class RequestHeader:
    request_api_key: ApiKey
    request_api_version: int
    correlation_id: int
    client_id: str | None

    @classmethod
    def decode (cls, readable: Readable) -> Self:
        request_header = cls(
            request_api_key=ApiKey.decode(readable),
            request_api_version=decode_int16(readable),
            correlation_id=decode_int32(readable),
            client_id=decode_nullable_string(readable)
        )

        decode_tagged_fields(readable)
        return request_header
    

@dataclass(frozen=True)
class Request(ABC):
    header: RequestHeader

    @classmethod
    @abstractmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> Self:
        raise NotImplementedError


# main function to decode request
def decode_request(readable: Readable) -> Request:
    header = RequestHeader.decode(readable)
    #request_class = None

    match header.request_api_key:
        case ApiKey.API_VERSIONS:
            from app.apis.api_versions import ApiVersionsRequest
            request_class = ApiVersionsRequest
        case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
            from app.apis.api_describe_topic_partitions import DescribeTopicPartitionsRequest
            request_class = DescribeTopicPartitionsRequest
            
    # if request_class is None:
    #     raise ValueError(f"Unknown request API key {header.request_api_key}")
    # TODO: Handle in a different way because we can return error code in response

    return request_class.decode_body(header, readable)

