import json
import uuid
from utils import get_logger
from core.DHTService import DHTService
from typing import List
from core.Peer import Peer
from queue import Queue
from socket import *
import threading
import globals

from enum import Enum

class Server:
    class ClientState(Enum):
        CLOSE=1
        SEND_HASH_TABLE=2
        ADD_PEER=3
        REMOVE_PEER=4
        DELETE_PEER=5
        UPDATE_PEER=6
        GET_PEER=7
        SEND_IDENTITY=8
        
    class ServerMessage():
        class MessageAction(Enum):
            ADD_PEER="peer add"
            REMOVE_PEER="peer remove"
            UPDATE_PEER="peer update"
            GET_PEER="peer get"
            SEND_IDENTITY="peer id"
        class MessageResult(Enum):
            ERROR="error"    
            COMPLETED="completed"
        
        def __init__(self) -> None:
            self.action: Server.ServerMessage.MessageAction = None
            self.result: Server.ServerMessage.MessageResult = None
            self.data: dict = None
            
        def to_dict(self) -> dict[str, str]:
            return { "action": self.action.value, "result": self.result.value,"data": self.data  }
    
    def __init__(self, port: int, host: str, dht_service: DHTService):
        self.logger                                     = get_logger("ServerLogger")
        self.dht_service                                = dht_service
        self.__consumers                                = globals.CONSUMERS_QUANTITY
        self.__client_list: List[threading.Thread]      = list()
        self.__consumers_list: List[threading.Thread]   = list()
        self.__server_address                           = (host, port)
        self.__socket                                   = socket(AF_INET, SOCK_STREAM)
        self.__queue: Queue[Peer.ptuple]                = Queue()
        self.__identity: Peer                           = None
    
    def start(self) -> None: 
        self.logger.info("(+) Server started.")
        self.__socket.bind(self.__server_address)
        self.__create_consumers()
        self.__socket.listen(globals.SOCKETS_CONNECTION_LIMIT)
        
        try:
            # loads the current identity of the peer
            with open(globals.IDENTITY_FILE, "r") as file:
                result = Peer.load_identity(file)
        except Exception as e:
            print("Creating a peer identity for you: ")
            with open(globals.IDENTITY_FILE, "w+") as file:
                name            = input("Peer Name: ")
                ip              = input("Peer Ip: ")
                user_sync_port  = int(input("User Sync Port: "))
                file_sync_port  = int(input("File Sync Port: "))
                network_port    = int(input("Network Port: "))
                consensus_port  = int(input("Consensus Port: "))
                # create a peer identity for this peer
                peer_id = uuid.uuid1()
                peer = Peer(
                    peer_id=str(peer_id),
                    name=name,
                    ip=ip,
                    ports={
                        "userSync": user_sync_port,
                        "fileSync": file_sync_port,
                        "networkLayer": network_port,
                        "consensusPort": consensus_port
                    }
                )
                self.__identity = peer
                file.write(json.dumps(peer.serialize()))
                self.dht_service.create_peer(self.__identity)
        
        while True:
            connection, address = self.__socket.accept()
            client_thread = threading.Thread(target=self.__handle_client, args=(connection, address))
            self.__client_list.append(client_thread)
            client_thread.start()
        
    def __handle_client(self, connection: socket, address: tuple) -> None:
        alive=True
        while alive:
            try:
                bin = connection.recv(1024)
                self.logger.info(f"(*) received {bin} from {address} size: {len(bin)}")
                messages:list[bytes] = bin.split(b"\n")
                for data in messages:
                    if len(data) > 2:
                        data = data.decode(globals.BASIC_DECODER)
                        self.logger.info(f"(*) Decoded message {data}")
                        data: dict = json.loads(data)
                        message_type = int(data.get("message_type", -1))
                        bdata = data.get("data", dict())
                        if message_type == Server.ClientState.CLOSE.value:   
                            alive=False
                            connection.close()
                            self.logger.info(f"(*) Clossing connection to {address}")
                        else: 
                            self.__queue.put((message_type, bdata, address, connection))
                            self.logger.info(f"(*) {address} request code: {message_type} on queue")
            except json.decoder.JSONDecodeError as e:
                alive=False
                connection.close()
                self.logger.error(f"Invalid JSON data received from {address}: {e}")
            except Exception as e:
                connection.close()
                alive = False
                self.logger.error(f"Unexpected error handling client {address}: {e}")
            
    def __create_consumers(self) -> None:
        def consumer() -> None:
            while True:
                message_type, data, address, connection = self.__queue.get()
                if message_type == Server.ClientState.SEND_HASH_TABLE.value:
                    self.logger.info(f"(*) {address} request: sending hash-table without code")
                    result = self.dht_service.get_hash_table()
                    connection.sendall(result)
                else:
                    message = Server.ServerMessage()
                    peer_id = data.get("peer_id", "")
                    ip = data.get("ip", "")
                    name = data.get("name", "")
                    ports = data.get("ports", "")
                    peer = Peer(ip=ip, name=name, ports=ports, peer_id=peer_id)
                    try:
                        result = False
                        if message_type == Server.ClientState.REMOVE_PEER.value:
                            message.action = Server.ServerMessage.MessageAction.REMOVE_PEER
                            result = self.dht_service.remove_peer(peer)
                            self.logger.info(f"(*) {address} request: removing peer result: {result} ")
                        elif message_type == Server.ClientState.SEND_IDENTITY.value:
                            message.action = Server.ServerMessage.MessageAction.SEND_IDENTITY
                            result = self.__identity.serialize()
                            self.logger.info(f"(*) {address} request: sending identity: {result}")
                        elif message_type == Server.ClientState.ADD_PEER.value:
                            message.action = Server.ServerMessage.MessageAction.ADD_PEER
                            result = self.dht_service.create_peer(peer)
                            self.logger.info(f"(*) {address} request: adding peer result: {result} ")
                            message.data = result
                        elif message_type == Server.ClientState.GET_PEER.value:
                            message.action = Server.ServerMessage.MessageAction.GET_PEER
                            result: dict[str, str] = self.dht_service.get_peer(peer_id)
                            self.logger.info(f"(*) {address} request: getting peer result: {peer} ")
                            message.data = result
                        elif message_type == Server.ClientState.UPDATE_PEER.value:
                            message.action = Server.ServerMessage.MessageAction.UPDATE_PEER
                            result = self.dht_service.update_peer(peer)
                            self.logger.info(f"(*) {address} request: updating peer result: {result}")
                            message.data = result
                        message.result = Server.ServerMessage.MessageResult.COMPLETED if result else Server.ServerMessage.MessageResult.ERROR
                    except Exception as e:
                        message.result = message.MessageResult.ERROR
                    message = json.dumps(message.to_dict()).encode(globals.BASIC_DECODER)
                    try:
                        connection.sendall(message)
                    except BrokenPipeError as e:
                        self.logger.error("Clossing connection --resolution: broken-pipe: client must closed the client-connection")
                        connection.close()
        for _ in range(self.__consumers):
            consumer_thread = threading.Thread(target=consumer)
            self.__consumers_list.append(consumer_thread)
            consumer_thread.start()

if __name__ == "__main__":
    server = Server(port=globals.PORT, host=globals.HOST, dht_service=globals.injector.get(DHTService))
    server.start()