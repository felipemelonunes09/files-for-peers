from datetime import datetime
import inspect
import json
import socket
import threading
from typing import Any, Callable, Type, TypeVar, Generic
from abc import ABCMeta, abstractmethod
from functools import wraps
from enum import Enum

T = TypeVar('T')

class QuickServerMap():
    CLOSE=-1
    PING=0

class ClientConnection():
    def __init__(self, socket: socket.socket, address: tuple[str, int]) -> None:
        self.__socket = socket
        self.__address = address

    def package(self) -> bytes:
        recv = self.__socket.recv(1024)
        return recv

class Thread():
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            t = threading.Thread(target=func, args=args, kwargs=kwargs)
            t.start()
            print("(+) Thread started for function ", func.__name__)
            return func(*args, **kwargs)
        return wrapper

class Prototype():
    class Property(Generic[T]):
        @abstractmethod
        def parse(self, value: Any) -> T:
            pass

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
            print(value)
            return value

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
    
class JsonMap():
    def __init__(self):
        self.__mapper: dict[str, dict[str, Prototype.Property]] = dict()
        super().__init__()

    def __call__(self, func: Callable) -> Callable:
        signature = inspect.signature(func)
        for param_name, param in signature.parameters.items():
            param_type = param.annotation if param.annotation != inspect.Parameter.empty else None
            if isinstance(param_type, type):
                bases = [base.__name__ for base in param_type.__bases__]
                if Prototype.__name__ in bases:
                    print(f"(+) Mapping parameter {param_name} as {param_type.__name__} Prototype")
                    self.__mapper[param_name] = self.buildMapper(param.annotation)
                    raise KeyError()

        @wraps(func)
        def wrapper(s, package: dict, *a, **k):
            kwargs = dict()
            for key in self.__mapper:
                instance = Prototype()
                for attr in self.__mapper[key]:
                    setattr(instance, attr, self.__mapper[key][attr].parse(package[key][attr]))
                kwargs[key] = instance
            return func(s, *a, **kwargs, **k)
        return wrapper
    
    def buildMapper(self, annotation: Type) -> Type:
        mapper = dict()
        for attr in annotation.__dict__:
            if not attr.startswith('__'):  # Filter out special methods and attributes
                property = getattr(annotation, attr, None)
                if isinstance(property, Prototype.Property):
                    print(f"\t(*) Mapping attribute {attr} as {property.__class__}")
                    mapper[attr] = annotation.__dict__[attr]
        return mapper
    
class QServer():
    def __init__(self, interface: str, port: int) -> None:
        self.__port = port
        self.__inteface = interface
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.__newConnectionDecoder:    Callable = None
        self.__newConnectionLoader:     Callable = None
        self.__newConnectionValueMap:   Callable = None

    def start(self) -> None:
        self.__socket.bind((self.__inteface, self.__port))
        self.__socket.listen(5)
        print(f"(+) Server started on {self.__inteface}:{self.__port}")
        while True:
            client_socket, address = self.__socket.accept()
            print(f"(+) Connection from {address}")
            self.onClientConnection(ClientConnection(client_socket, address))

    def setOnNewConnection(self, decoder: Callable, loader: Callable, valueMap: Callable) -> None:
        self.__newConnectionDecoder  = decoder
        self.__newConnectionLoader   = loader
        self.__newConnectionValueMap = valueMap

    def onClientConnection(self, clientConnection: ClientConnection):
        pack = clientConnection.package()
        if self.__newConnectionDecoder:
            decodePackage = self.__newConnectionDecoder(pack)
        
        if self.__newConnectionLoader:
            loadPackage = self.__newConnectionLoader(decodePackage)
        
        if self.__newConnectionValueMap:
            mappedCode = self.__newConnectionValueMap(loadPackage)

        self.onMappedCode(mappedCode, package=loadPackage)

    def onMappedCode(self, code: T, package: Any):
        mappedFunction = Map.getMappedFunction(code)
        mappedFunction(self, package)

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
    
