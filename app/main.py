
import asyncio
from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass
from io import BytesIO
from typing import Self
import time

from .protocol.request import Request, decode_request
from .protocol.response import Response, handle_request


class KafkaServer:

    async def run(self) -> None:
        server = await asyncio.start_server(self._handle_client_connection_cb, host="localhost", port=9092, reuse_port=True)
        async with server:
            print("Starting server")
            await server.serve_forever()
    
    async def _handle_client_connection_cb(self, reader: StreamReader, writer: StreamWriter) -> None:
        print("Client connected")
        async with KafkaConnectionHandler(reader, writer) as handler:
            while True:
                try:
                    request: Request = await handler.receive_request()
                    print(f"Received request: {request}")
                except Exception as _:
                    break

                response: Response = handle_request(request)
                print(f"Sending response: {response}")
                await handler.send_response(response)
                #break
            # When testing below code with nc, always use with -q argument which closes the socket after EOF on stdin
            # ex: echo "test" | nc -q 1  localhost 9092
            # request = await reader.read()
            # print("received Message!")
            # message_size = 4
            # correlation_id = 7
            # writer.write(message_size.to_bytes(4) + correlation_id.to_bytes(4))
            # await writer.drain()
            # print("Sent response!")
        
        

#@dataclass(frozen=True)
class KafkaConnectionHandler:
    # _reader: StreamReader
    # _writer: StreamWriter

    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    async def receive_request(self) -> Request:
        message_size = int.from_bytes(await self._reader.readexactly(4))
        readable = BytesIO(await self._reader.readexactly(message_size))
        return decode_request(readable)

    async def send_response(self, response: Response) -> None:
        data = response.encode()
        self._writer.write(len(data).to_bytes(4) + data)
        await self._writer.drain()

    async def __aenter__(self) -> Self:
        print(f"+++++++++++ New Connection: {self._writer.get_extra_info('peername')} +++++++++++")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self._writer.close()
        await self._writer.wait_closed()
        print(f"----------- Connection Closed: {self._writer.get_extra_info('peername')} -----------")

if __name__ == "__main__":
    asyncio.run(KafkaServer().run())
