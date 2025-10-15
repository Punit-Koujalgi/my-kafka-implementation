from dataclasses import dataclass
from typing import Self

from kafka_server.protocol.protocol import *
from kafka_server.protocol.request import RequestHeader, Request
from kafka_server.protocol.response import ResponseHeader, Response


@dataclass(frozen=True)
class ApiVersionsRequest(Request):
    client_software_name: str
    client_software_version: str

    @classmethod
    def decode_body(cls, header: RequestHeader, readable: Readable) -> Self:
        request = cls(
            header=header,
            client_software_name=decode_compact_string(readable),
            client_software_version=decode_compact_string(readable)
        )
        decode_tagged_fields(readable)
        return request


@dataclass(frozen=True)
class ApiVersion:
    api_key: ApiKey
    min_version: int
    max_version: int

    def encode(self) -> bytes:
        return (
            self.api_key.encode() +
            encode_int16(self.min_version) +
            encode_int16(self.max_version) +
            encode_tagged_fields()
        )
        ''' (works same)
        return b"".join([
            self.api_key.encode(),
            encode_int16(self.min_version),
            encode_int16(self.max_version),
            encode_tagged_fields(),
        ])
        '''

@dataclass(frozen=True)
class ApiVersionsResponse(Response):
    error_code: ErrorCode
    api_keys: list[ApiVersion]
    throttle_time_ms: int

    def _encode_header(self) -> bytes:
        return self.header.encode(version=0)
    
    def _encode_body(self) -> bytes:
        return b"".join([
            self.error_code.encode(),
            encode_compact_array(self.api_keys),
            encode_int32(self.throttle_time_ms),
            encode_tagged_fields()
        ])


def handle_api_versions_request(request: ApiVersionsRequest) -> ApiVersionsResponse:
    if request.header.request_api_version == 4:
        error_code = ErrorCode.NONE
    else:
        error_code = ErrorCode.UNSUPPORTED_VERSION

    return ApiVersionsResponse(
        header=ResponseHeader.from_request_header(request.header),
        error_code=error_code,
        api_keys=[
            ApiVersion(ApiKey.PRODUCE, min_version=0, max_version=11),
            ApiVersion(ApiKey.METADATA, min_version=0, max_version=12),
            ApiVersion(ApiKey.API_VERSIONS, min_version=4, max_version=4),
            ApiVersion(ApiKey.CREATE_TOPICS, min_version=7, max_version=7),
            ApiVersion(ApiKey.DESCRIBE_TOPIC_PARTITIONS, min_version=0, max_version=0),
            ApiVersion(ApiKey.FETCH, min_version=16, max_version=16),
        ],
        throttle_time_ms=0
    )


