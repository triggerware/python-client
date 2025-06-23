import atexit
import time
import select
from io import BytesIO
import json
import socket
import threading
from typing import Any, Callable
from threading import Condition

class JsonRpcMessageHandler:
    execute: Callable[[dict[str, Any] | list[Any]], dict[str, Any]]
    notify: Callable[[dict[str, Any] | list[Any]], None]

    def __init__(
        self,
        execute: Callable[[dict[str, Any] | list[Any]], dict[str, Any]],
        notify: Callable[[dict[str, Any] | list[Any]], None]):
        self.execute = execute
        self.notify = notify


class JsonRpcClient:
    address: str
    port: int
    closed: bool = False
    current_id: int = 0
    methods: dict[str, JsonRpcMessageHandler] = {}
    incoming: dict[int, dict[str, Any]] = {}
    buffer: BytesIO = BytesIO()
    buffer_idx: int = 0
    buffer_parsing: bool = False
    decoder: json.JSONDecoder = json.JSONDecoder()

    # outgoing_lock: Condition = Condition()
    incoming_lock: Condition = Condition()
    method_lock: Condition = Condition()

    def __init__(self, address: str, port: int):
        self.address = address
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((address, port))
        threading.Thread(target=self.start_read_thread,daemon=True).start()
        # threading.Thread(target=self.start_write_thread, daemon=True).start()
        atexit.register(self.close)

    def call(self, method: str, params: dict[str, Any] | list[Any]) -> Any:
        """
        Execute a method on the server and return the result. Called synchronously - will block the
        main thread until a response is received.

        :param method: The method to call on the server.
        :param params: The parameters to send to the server. Can either be named or positional.
        :return: The result of the method call.
        :raises JsonRpcException: If the server returns an error.
        """
        next_id = self.next_id()
        message = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": next_id
        }
        self.socket.send(json.dumps(message).encode('utf-8'))

        with self.incoming_lock:
            while next_id not in self.incoming:
                if self.closed:
                    raise ServerErrorException("Connection closed before response received")
                self.incoming_lock.wait()

            result = self.incoming.pop(next_id)

        if not result:
            return None
        if "error" in result:
            raise JsonRpcException(result["error"]["message"], result["error"]["code"])
        elif "result" in result:
            return result["result"]
        else:
            raise ServerErrorException("Server sent an invalid response")

    def notify(self, method: str, params: dict[str, Any] | list[Any]):
        """
        Notifies the server of an event. Notifications do not require responses, so this will not
        block the main thread.

        :param method: The method to notify the server of.
        :param params: The parameters to send to the server. Can either be named or positional.
        """
        message = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }
        self.socket.send(json.dumps(message).encode('utf-8'))

    def add_method(self, method: str, handler: JsonRpcMessageHandler):
        """
        Add a method to the client. The handler will be called whenever the server sends a request.
        :param method: The method to add.
        :param handler: The handler to call when the method is called.
        """
        with self.method_lock:
            self.methods[method] = handler

    def remove_method(self, method: str):
        """
        Remove a method from the client.
        :param method: The method to remove.
        """
        with self.method_lock:
            del self.methods[method]

    def start_read_thread(self):
        while not self.closed:
            try:
                ready_to_read, _, _ = select.select([self.socket], [], [])
                if ready_to_read:
                    data = self.socket.recv(2048)
                    if not data:
                        self.close()
                        break

                    self.buffer.seek(0, 2)
                    self.buffer.write(data)

                while not self.closed:
                    try:
                        self.buffer.seek(0)
                        position = self.buffer.tell()
                        while self.buffer.read(1).isspace():
                            position += 1
                            pass
                        self.buffer.seek(position)

                        obj, pos = self.decoder.raw_decode(self.buffer.read().decode('utf-8'))
                        if obj["jsonrpc"] == "2.0":
                            print("   GOT A MESSAGE! It was ", obj)
                            pass
                        else:
                            raise InvalidRequestException()
                    except json.JSONDecodeError as e:
                        break

                    remaining = self.buffer.getvalue()[pos+1:]

                    if "id" in obj:
                        if "method" in obj:
                            if obj["method"] in self.methods:
                                response = self.methods[obj["method"]].execute(obj["params"])
                            else:
                                response = {
                                    "jsonrpc": "2.0",
                                    "id": obj["id"],
                                    "error": {
                                        "code": -32601,
                                        "message": f"Method '{obj['method']}' not found"
                                    }
                                }
                            self.socket.send(json.dumps(response).encode('utf-8'))
                        else:
                            with self.incoming_lock:
                                self.incoming[obj["id"]] = obj
                                self.incoming_lock.notify_all()
                    elif "method" in obj:
                        with self.method_lock:
                            if obj["method"] in self.methods:
                                self.methods[obj["method"]].notify(obj["params"])
                            self.method_lock.notify_all()

                    self.buffer = BytesIO(remaining)
                    self.buffer.seek(0, 2)

            except (ConnectionResetError, OSError):
                if not self.closed:
                    self.close()
                    raise ServerErrorException("Connection lost unexpectedly.")
            except Exception as e:
                self.close()
                raise InternalErrorException("Error receiving messages") from e

    def next_id(self) -> int:
        new_id = self.current_id
        self.current_id += 1
        return new_id

    def close(self):
        print("closing JSON-RPC client...")
        self.closed = True
        self.socket.close()


class JsonRpcException(Exception):
    def __init__(self, message: str, code: int):
        super().__init__(message)
        self.message = message
        self.code = code

    def to_json_rpc_error(self):
        return {"code": self.code, "message": str(self)}


class ParseErrorException(JsonRpcException):
    def __init__(self):
        super().__init__("Error parsing JSON-RPC message", -32700)


class InvalidRequestException(JsonRpcException):
    def __init__(self):
        super().__init__("Invalid JSON-RPC message", -32600)


class MethodNotFoundException(JsonRpcException):
    def __init__(self, method: str):
        super().__init__(f"Method not found: {method}", -32601)


class InvalidParamsException(JsonRpcException):
    def __init__(self):
        super().__init__("Invalid params", -32602)


class InternalErrorException(JsonRpcException):
    def __init__(self, message: str):
        super().__init__(message, -32603)


class ServerErrorException(JsonRpcException):
    def __init__(self, message: str):
        super().__init__(message, -32000)


