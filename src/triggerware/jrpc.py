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
        """
        Initialize a JsonRpcMessageHandler.

        Args:
            execute (Callable): Function to execute a request.
            notify (Callable): Function to handle notifications.
        """
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
        """
        Initialize a JsonRpcClient and connect to the server.

        Args:
            address (str): The server address.
            port (int): The server port.
        """
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

        Args:
            method (str): The method to call on the server.
            params (dict[str, Any] | list[Any]): The parameters to send to the server. Can either be named or positional.

        Returns:
            Any: The result of the method call.

        Raises:
            JsonRpcException: If the server returns an error.
            ServerErrorException: If the connection is closed before response is received or server sends invalid response.
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

        Args:
            method (str): The method to notify the server of.
            params (dict[str, Any] | list[Any]): The parameters to send to the server. Can either be named or positional.
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

        Args:
            method (str): The method to add.
            handler (JsonRpcMessageHandler): The handler to call when the method is called.
        """
        with self.method_lock:
            self.methods[method] = handler

    def remove_method(self, method: str):
        """
        Remove a method from the client.

        Args:
            method (str): The method to remove.
        """
        with self.method_lock:
            del self.methods[method]

    def start_read_thread(self):
        """
        Starts the thread that reads incoming messages from the server.
        """
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
                            # print("   GOT A MESSAGE! It was ", obj)
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
        """
        Returns the next available request ID.

        Returns:
            int: The next request ID.
        """
        new_id = self.current_id
        self.current_id += 1
        return new_id

    def close(self):
         """
         Close the client and log where the request came from.
         """
         # traceback.print_stack(limit=5)
         self.closed = True
         self.socket.close()

class JsonRpcException(Exception):
    def __init__(self, message: str, code: int):
        """
        Exception for JSON-RPC errors.

        Args:
            message (str): The error message.
            code (int): The error code.
        """
        super().__init__(message)
        self.message = message
        self.code = code

    def to_json_rpc_error(self):
        """
        Converts the exception to a JSON-RPC error dictionary.

        Returns:
            dict: The error as a JSON-RPC error object.
        """
        return {"code": self.code, "message": str(self)}


class ParseErrorException(JsonRpcException):
    def __init__(self):
        """
        Exception for JSON-RPC parse errors.
        """
        super().__init__("Error parsing JSON-RPC message", -32700)


class InvalidRequestException(JsonRpcException):
    def __init__(self):
        """
        Exception for invalid JSON-RPC requests.
        """
        super().__init__("Invalid JSON-RPC message", -32600)


class MethodNotFoundException(JsonRpcException):
    def __init__(self, method: str):
        """
        Exception for method not found errors.

        Args:
            method (str): The method name that was not found.
        """
        super().__init__(f"Method not found: {method}", -32601)


class InvalidParamsException(JsonRpcException):
    def __init__(self):
        """
        Exception for invalid parameters in JSON-RPC requests.
        """
        super().__init__("Invalid params", -32602)


class InternalErrorException(JsonRpcException):
    def __init__(self, message: str):
        """
        Exception for internal errors in JSON-RPC.

        Args:
            message (str): The error message.
        """
        super().__init__(message, -32603)


class ServerErrorException(JsonRpcException):
    def __init__(self, message: str):
        """
        Exception for server errors in JSON-RPC.

        Args:
            message (str): The error message.
        """
        super().__init__(message, -32000)


