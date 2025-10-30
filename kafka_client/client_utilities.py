
import socket
from uuid import UUID
import struct


def read_response_v1(sock: socket.socket) -> bytes:
    """Read a response from the socket."""
    # Read message length
    length_bytes = sock.recv(4)
    if len(length_bytes) != 4:
        raise Exception("Failed to read message length")
    
    message_length = struct.unpack(">I", length_bytes)[0]
    
    # Read message
    message = b""
    while len(message) < message_length:
        chunk = sock.recv(message_length - len(message))
        if not chunk:
            raise Exception("Connection closed")
        message += chunk
    
    return message

class ServerConnector:
    """Class to manage Kafka server connection."""
    
    def __init__(self):
        self.host = "localhost" # Kafka broker host
        self.port = 9092        # Kafka broker port
        self.timeout = 5
        
    def send(self, request: bytes) -> bytes:
        """Send request and get response using a fresh connection."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect((self.host, self.port))
            sock.send(request)
            response = read_response_v1(sock)
            return response
        except (socket.timeout, ConnectionRefusedError):
            raise ConnectionError("Failed to connect to Kafka broker")
        finally:
            sock.close()

def connect_to_server():
    """Attempt to connect to the Kafka server."""
    return ServerConnector()



