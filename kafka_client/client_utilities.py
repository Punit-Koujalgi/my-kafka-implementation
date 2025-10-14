
import socket

from .request_builder import *
from .response_parser import *

class ServerConnector:
    """Class to manage Kafka server connection."""
    _instance = None
    
    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            print("Creating ClusterMetadata singleton instance")
        return cls._instance

    def __init__(self):
        self.host = "localhost" # Kafka broker host
        self.port = 9092        # Kafka broker port
        self.timeout = 5
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        try:
            self.sock.connect((self.host, self.port))
        except (socket.timeout, ConnectionRefusedError):
            raise ConnectionError("Failed to connect to Kafka broker")
        
    def send(self, request: bytes) -> bytes:
        self.sock.send(request)
        response = read_response(self.sock)
        return response

def connect_to_server():
    """Attempt to connect to the Kafka server."""
    return ServerConnector()

def convert_topic_name_to_uuid(topic_name: str) -> UUID:
    """Convert topic name to UUID"""

    describe_request = create_metadata_request([topic_name])
    response = ServerConnector().send(describe_request)
    parsed_response = parse_metadata_response(response)

    return parsed_response[topic_name]['topic_id'] # type: ignore



