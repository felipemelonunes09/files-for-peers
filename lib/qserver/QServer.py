from datetime import datetime
import inspect
import json
from queue import Queue
import socket
import struct
import threading
from typing import Any, Callable, Type, TypeVar, Generic
from abc import ABCMeta, abstractmethod
from functools import wraps

T = TypeVar('T')
SOCKET_BUFFER = 1024

class QuickServerMap():
    CLOSE=-1
    PING=0

class MessagePolicy():
    @abstractmethod
    def receivePackage(self) -> bytes:
        pass

    @abstractmethod
    def buildPackage(self) -> bytes:
        pass


class FixedSizeMessagePolicy(MessagePolicy):
    ...

class DelimiterMessagePolicy(MessagePolicy):
    ...

class HeaderMessagePolicy(MessagePolicy):
    
    def __init__(self, headerLength: int = 4):
        self.headerLenght = headerLength
        self.minBytes = headerLength
        super().__init__()

    def receivePackage(self, socket: socket.socket) -> bytes:
        data = socket.recv(self.minBytes)
        header = data[:self.headerLenght]
        data = data[self.headerLenght:]

        if len(header) < self.headerLenght:
            return b''
        messageSize = struct.unpack('>I', header)[0]
        while len(data) < messageSize and socket.fileno() != -1:
            chunk = socket.recv(messageSize - len(data))
            if not chunk:
                return b''
            data += chunk
        return data

    def buildPackage(self, message: bytes) -> None:
        header = struct.pack('>I', len(message))
        return header + message

class ClientConnection():
    def __init__(self, socket: socket.socket, address: tuple[str, int], bufSize: int = SOCKET_BUFFER, messagePolicy: MessagePolicy = None) -> None:
        self.__socket = socket
        self.__address = address
        self.__bufferSize = bufSize
        self.__messagePolicy = messagePolicy

    def package(self) -> bytes:
        if not self.__messagePolicy:
            recv = self.__socket.recv(self.__bufferSize)
            return recv
        return self.__messagePolicy.receivePackage(self.__socket)

    def close(self):
        self.__socket.close()
        
class Thread():
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            t = threading.Thread(target=func, args=args, kwargs=kwargs)
            print("(+) Thread started for function", func.__name__)
            t.start()
        return wrapper

class Prototype():
    class Property(Generic[T]):
        @abstractmethod
        def parse(self, value: Any) -> T:
            pass
        
    class Boolean(Property[bool]):
        def parse(self, value):
            return bool(value)

    class Int(Property[int]):
        def parse(self, value):
            return int(value)

    class String(Property[str]):
        def parse(self, value):
            return str(value)

    class DateTime(Property[datetime]):
        def parse(self, value):
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

    class Dict(Property[dict[str, Any]]):
        
        def __init__(self, *args, **kwd):
            super().__init__()

        def parse(self, value):
            compose = dict(value)
            return compose

class Map(Generic[T]):
    __registry: dict[T, Callable] = dict()
    def __init__(self, code: T) -> None:
        self.code = code

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(s, *args, **kwargs):
            return func(s, *args, **kwargs)
        
        print(f"(+) Function {func.__name__} mapped with code {self.code}")
        Map.__registry[self.code] = wrapper
        return wrapper
    
    @classmethod
    def getMappedFunction(cls, code: T) -> Callable:
        if code not in cls.__registry:
            raise KeyError(f'Code {code} not mapped in @Map decorator')
        return cls.__registry[code]

class ParamMap():
    def __init__(self, isproperty: bool, property: Prototype.Property, scheme: dict[str, Prototype.Property]):
        self.isproperty = isproperty
        self.property = property
        self.scheme = scheme

class JsonMap():
    def __init__(self):
        self.__mapper: dict[str, ParamMap] = dict()
        super().__init__()

    def __call__(self, func: Callable) -> Callable:
        signature = inspect.signature(func)
        for param_name, param in signature.parameters.items():
            param_type = param.annotation if param.annotation != inspect.Parameter.empty else None
            if isinstance(param_type, type):
                bases = [base.__name__ for base in param_type.__bases__]
                if Prototype.__name__ in bases or Prototype.Property.__name__ in bases:
                    print(f"(+) Mapping parameter {param_name} as {param_type.__name__} Prototype to function [{func.__name__}]")
                    self.__mapper[param_name] = self.buildMapper(param.annotation)

        @wraps(func)
        def wrapper(s, package: dict, *a, **k):
            kwargs = dict()
            for key in self.__mapper:
                attribute_map = self.__mapper[key]
                if attribute_map.isproperty:
                    kwargs[key] = self.__mapper[key].property.parse(value=package[key])
                else:
                    instance = Prototype()
                    for attr in self.__mapper[key].scheme:
                        setattr(instance, attr, self.__mapper[key].scheme[attr].parse(package[attr]))
                    kwargs[key] = instance
            return func(s, *a, **kwargs, **k)
        return wrapper
    
    def buildMapper(self, annotation: Type) -> ParamMap:
        bases = list(annotation.__bases__)
        isproperty = Prototype.Property in bases
        scheme = dict()
        property = None
        if isproperty:
            print(f"\t(*) Mapping property {annotation}")
            property = annotation()
        else:
            for attr in annotation.__dict__:
                if not attr.startswith('__'):  # Filter out special methods and attributes
                    prop = getattr(annotation, attr, None)
                    if isinstance(prop, Prototype.Property):
                        print(f"\t(*) Mapping attribute {attr} as {prop.__class__}")
                        scheme[attr] = prop
        return ParamMap(isproperty=isproperty, property=property, scheme=scheme)
    
class MappedCallHandler():
    @abstractmethod
    def call(self, code: Any, target: Callable, reference: Type, *args):
        pass

class SimpleCallHandler(MappedCallHandler):
    def call(self, target: Callable, reference: Type, package: Any):
        target(s=reference, package=package)

class ConsumerFunctionHandler(MappedCallHandler, Generic[T]):
    def __init__(self, consumersQuantity: int):
        self.__consumersQuantity = consumersQuantity
        self.__pool: list[threading.Thread] = list()
        self.__queue: Queue[T, Callable, tuple] = Queue()

        for _ in range(self.__consumersQuantity):
            thread = threading.Thread(target=self.consume)
            self.__pool.append(thread)
            print(f"(+) Consumers thread started id: {id(thread)}")
            thread.start()

    def call(self, code: T, target: Callable, reference: Type, *args):
        self.__queue.put((code, target, reference, *args))
        return super().call(code, target, args)

    def consume(self):
        while True:
            code, target, reference, args = self.__queue.get()
            target(s=reference, package=args)

class QServer():
    def __init__(self, interface: str, port: int, keepAlive: bool = False, mappedFunctionHandler: MappedCallHandler = SimpleCallHandler(), messagePolicy: MessagePolicy = FixedSizeMessagePolicy() ) -> None:
        self.__port = port
        self.__inteface = interface
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.__newConnectionDecoder:    Callable = None
        self.__newConnectionLoader:     Callable = None
        self.__newConnectionValueMap:   Callable = None

        self.__mappedFunctionHandler = mappedFunctionHandler
        self.__messagePolicy: MessagePolicy = messagePolicy
        self.__keepAlive = keepAlive

    def start(self) -> None:
        self.__socket.bind((self.__inteface, self.__port))
        self.__socket.listen(5)
        print(f"(+) Server started on {self.__inteface}:{self.__port}")
        while True:
            client_socket, address = self.__socket.accept()
            print(f"(+) Connection from {address}")
            #thread = threading.Thread(target=self.onClientConnection, args=(ClientConnection(client_socket, address, messagePolicy=self.__messagePolicy),))
            #thread.start()
            self.onClientConnection(ClientConnection(client_socket, address, messagePolicy=self.__messagePolicy))

    def setOnNewConnection(self, decoder: Callable, loader: Callable, valueMap: Callable) -> None:
        self.__newConnectionDecoder  = decoder
        self.__newConnectionLoader   = loader
        self.__newConnectionValueMap = valueMap

    def onClientConnection(self, clientConnection: ClientConnection):
        keepAlive = self.__keepAlive
        patience = 5
        patienceCount = 0
        while keepAlive and (patienceCount <= patience):
            pack = clientConnection.package()
            if len(pack) == 0:
                patienceCount += 1
            else:
                # create a method to decode using to package
                if self.__newConnectionDecoder:
                    decodePackage = self.__newConnectionDecoder(pack)
                    
                # create a method to load using to package
                if self.__newConnectionLoader:
                    loadPackage = self.__newConnectionLoader(decodePackage)
                    
                # create a method to map using to package
                if self.__newConnectionValueMap:
                    mappedCode = self.__newConnectionValueMap(loadPackage)

                print(f"(+) Sending to mapped function")
                self.onMappedCode(mappedCode, package=loadPackage)
        clientConnection.close()
        print("(+) Connection closed")
        return None
    def onMappedCode(self, code: Any, package: Any):
        mappedFunction = Map.getMappedFunction(code)
        self.__mappedFunctionHandler.call(code, mappedFunction, self, package)

    def setMessagePolicy(self, messagePolicy: MessagePolicy):
        self.__messagePolicy = messagePolicy

class QuickServer(QServer):
    @Thread()
    def onClientConnection(self, clientConnection: ClientConnection):
        return super().onClientConnection(clientConnection)

def utf8Decoder(bytes: bytes) -> str:
    return bytes.decode("utf-8")

def jsonLoader(data: str) -> dict:
    return json.loads(data)

def keyMap(key: str) -> Callable:
    def _map(dictionary: dict) -> str:
        return dictionary[key]
    return _map
    
