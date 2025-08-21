
from dataclasses import dataclass
from typing import Literal
from abc import ABC, abstractmethod

from .protocol import *
from .request import RequestHeader, Request

@dataclass(frozen=True)
class ResponseHeader:
    correlation_id: int

    def encode(self, version: Literal[0, 1]) -> bytes:
        data = encode_int32(self.correlation_id)
        if version == 1:
            data += encode_tagged_fields()
        return data
    

    @classmethod
    def from_request_header(cls, request_header: RequestHeader) -> Self:
        return cls(correlation_id=request_header.correlation_id)
    
@dataclass(frozen=True)
class Response(ABC):
    header: ResponseHeader

    def encode(self) -> bytes:
        return self._encode_header() + self._encode_body()
    
    def _encode_header(self) -> bytes:
        return self.header.encode(version=1)
    
    @abstractmethod
    def _encode_body(self) -> bytes:
        raise NotImplementedError
    
# orchestrator for handlers: invokes the correct handler
def handle_request(request: Request) -> Response:
    match request.header.request_api_key:
        case ApiKey.API_VERSIONS:
            from app.apis.api_versions import ApiVersionsRequest, handle_api_versions_request
            request_class, request_handler = ApiVersionsRequest, handle_api_versions_request

    assert isinstance(request, request_class)
    return request_handler(request)

