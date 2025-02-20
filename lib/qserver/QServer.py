from __future__ import annotations
import inspect
import json
import re
import socket
import struct
import threading
from datetime import datetime
from queue import Queue
from typing import Any, Callable, Self, Type, TypeVar, Generic
from abc import ABC, ABCMeta, abstractmethod
from functools import wraps

T = TypeVar('T')
SOCKET_BUFFER = 1024
DEBUG = True

def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

class QuickServerMap():
    CLOSE=-1
    PING=0

class MessagePolicy():
    @abstractmethod
    def receivePackage(self, socket: socket.socket) -> bytes:
        pass

    @abstractmethod
    def buildPackage(self, message: bytes, socket: socket.socket) -> bytes:
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

    def buildPackage(self, message: bytes) -> bytes:
        header = struct.pack('>I', len(message))
        return header + message
    
class Package():
    def __init__(self, payload: object, msg: str, statusCode: int):
        self.payload = payload
        self.msg = msg
        self.statusCode = statusCode

    def serialize(self) -> str:
        return f"{self.msg},{self.statusCode},{str(self.payload)}"

    def encode(self, encoding: str) -> bytes:
        return self.serialize().encode(encoding=encoding)

class JsonPackage(Package):
    def serialize(self):
        return json.dumps({
            "status": self.statusCode,
            "message": self.msg,
            "payload": self.payload
        })

class ClientConnection():
    def __init__(self, socket: socket.socket, address: tuple[str, int], bufSize: int = SOCKET_BUFFER, messagePolicy: MessagePolicy = None) -> None:
        self.__socket = socket
        self.__address = address
        self.__bufferSize = bufSize
        self.__messagePolicy = messagePolicy
        self.encoder = "utf-8"

    def package(self) -> bytes:
        if not self.__messagePolicy:
            recv = self.__socket.recv(self.__bufferSize)
            return recv
        return self.__messagePolicy.receivePackage(self.__socket)
    
    def sendPackage(self, package: Package):
        if not self.__messagePolicy:
            self.__socket.sendall(package.encode(self.encoder))
            return 
        bytes = self.__messagePolicy.buildPackage(message=package.encode(self.encoder))
        self.__socket.sendall(bytes)

    def sendAndClose(self, package: Package):
        self.sendPackage(package)
        self.close()

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

##
## Deprecated maybe could be used in the future
## Future implementation could be in PrototypeMap({ raisePrototypeExceptions: True })
## If the Validator class will be used in the future, use with the prototype.isValid() implemented on the PrototypeMap
## Or create a new class eg PrototypeExceptionMap()
##
class Validator():
    class SizeValidator():
        @staticmethod
        def validate(value: object, arguments: dict) -> tuple[bool, ValueError | None]:
            dprint(f"\t(*) Size validation of {value} with arguments: {arguments}")
            minSize = arguments.get("minSize", None)
            maxSize = arguments.get("maxSize", None)
            if minSize and len(value) < minSize:
                return False, ValueError("The value is less than the specified minimum " + str(minSize))
            if maxSize and len(value) > maxSize:
                return False, ValueError("The value is greater than the specified maximum " + str(maxSize))
            return True, None

    class PatternValidator():
        @staticmethod
        def validate(value: object, arguments: dict) -> tuple[bool, ValueError | None]:
            dprint(f"\t(*) Pattern validation of {value} with arguments: {arguments}")
            pattern = arguments.get("pattern", None)
            if pattern and not re.match(pattern, value):
                return False, ValueError("The value do not match with the pattern " + str(pattern))
            return True, None

class Prototype(): 

    def isValid(self) -> bool:
        properties = self.getProperties()
        scheme: dict[str, Prototype.Property] = getattr(self, "__scheme__", {})
        for key in scheme:
            attr = getattr(self, key, None)
            isValid, error = scheme[key].validate(attr)
            if error:
                return False
        return True

    def getProperties(self) -> list[Property]:
        properties: dict[str, Prototype.Property] = getattr(self, "__scheme__")
        return list(properties.values())

    class Property(Generic[T], ABC):
        def __init__(self, **k):
            self.arguments = k
            super().__init__()

        @abstractmethod
        def parse(self, value: Any) -> T:
            pass

        def validate(self, value: T) -> tuple[bool, ValueError | None]:
            return True, None

    class Boolean(Property[bool]):
        def parse(self, value):
            return bool(value)
    
    class Int(Property[int]):
        def parse(self, value):
            return int(value)

    class String(Property[str]):
        __validators = [Validator.SizeValidator, Validator.PatternValidator]
        def parse(self, value):
            return str(value)
        
        def validate(self, value) -> tuple[bool, ValueError | None]:
            for validator in self.__validators:
                isValid, error = validator.validate(value, self.arguments)
                if error:
                    return isValid, error
            return super().validate(value)

    class DateTime(Property[datetime]):
        def parse(self, value):
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        
        
    class Dict(Property[dict[str, Any]]):
        
        def __init__(self, *args, **kwd):
            super().__init__()

        def parse(self, value):
            compose = dict(value)
            return compose
        
    def __str__(self) -> str:
        return str(self.__dict__)
        

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

class PrototypeMap():
    def __init__(self):
        self.__mapper: dict[str, ParamMap] = dict()
        super().__init__()

    def __call__(self, func: Callable) -> Callable:
        signature = inspect.signature(func)
        for param_name, param in signature.parameters.items():
            param_type = param.annotation if param.annotation != inspect.Parameter.empty else None
            dprint(f"(*) Type: {param_type} inspected on signature ")
            if isinstance(param_type, type):
                dprint(f"\t (*) param_type is a instance of type with __mro__: {param_type.__mro__}")
                if Prototype in param_type.__mro__ or Prototype.Property in param_type.__mro__:
                    print(f"(+) Mapping parameter {param_name} as {param_type.__name__} Prototype to function [{func.__name__}]")
                    self.__mapper[param_name] = self.buildMapper(param.annotation)

        @wraps(func)
        def wrapper(reference, package, **k):
            kwargs = dict()
            for key in self.__mapper:
                attribute_map = self.__mapper[key]
                dprint(f"(*) Building for attribute {attribute_map}")
                if attribute_map.isproperty:
                    value = self.__mapper[key].property.parse(value=package[key])
                    kwargs[key] = value
                else:
                    instance = Prototype()
                    for attr in self.__mapper[key].scheme:
                        value = self.__mapper[key].scheme[attr].parse(package[attr])
                        setattr(instance, attr, value)
                    setattr(instance, "__scheme__", self.__mapper[key].scheme)
                    kwargs[key] = instance
            return func(reference, **kwargs, **k)
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
            #props = []
            for attr in annotation.__dict__:
                if not attr.startswith('__'):  # Filter out special methods and attributes
                    prop = getattr(annotation, attr, None)
                    if isinstance(prop, Prototype.Property):
                        print(f"\t(*) Mapping attribute {attr} as {prop.__class__}")
                        scheme[attr] = prop
                        #props.append(prop)
                    #scheme["__props__"] = props
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

    def call(self, code: T, target: Callable, reference: Type, functionParameters: dict):
        self.__queue.put((code, target, reference, functionParameters))
        return super().call(code, target, functionParameters)

    def consume(self):
        while True:
            code, target, reference, args = self.__queue.get()
            target(s=reference, **args)

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
                # create a method to decode using the package
                if self.__newConnectionDecoder:
                    decodePackage = self.__newConnectionDecoder(pack)
                    
                # create a method to load using the package
                if self.__newConnectionLoader:
                    loadPackage = self.__newConnectionLoader(decodePackage)
                    
                # create a method to map using the package
                if self.__newConnectionValueMap:
                    mappedCode = self.__newConnectionValueMap(loadPackage)

                print(f"(+) Sending to mapped function")
                self.onMappedCode(code=mappedCode, clientConnection=clientConnection, package=loadPackage)
        print("closing socket")
        clientConnection.close()
        print("(+) Connection closed")
        return None
    
    def onMappedCode(self, code: Any, **k):
        mappedFunction = Map.getMappedFunction(code)
        self.__mappedFunctionHandler.call(code, mappedFunction, self, k)

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
    
