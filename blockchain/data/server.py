from concurrent.futures import ThreadPoolExecutor
from core.chain.block import Block
from enum import Enum
from queue import Queue

import datetime
import hashlib
import os
import socket
import yaml
import json
import globals
import logging
import threading 


class Server():
    class BlockHashSHA256Generator():
        def __call__(self, block: Block) -> str:
            block = block.serialize()
            encoded_block = json.dumps(block).encode(globals.ENCODING)
            hash_block = hashlib.sha256(encoded_block)
            return hash_block.hexdigest()
     
    class MessageType(Enum):
        SEND_BLOCK_CHAIN_CHUNK=1
        SEND_BLOCK_CHAIN=2
        ADD_BLOCK=3
        SEND_LAST_BLOCK_CHAIN_CHUNK=4
        CLOSE=-1
            
    class ChainManager():   
        def __init__(self, path: str) -> None:
            self.__path                         = path
            self.__chain:list[Block]            = list()
            self.__files:list[str]              = list()
            self.__chunks:list[dict[str, list]]      = list()
                
        def get_chain(self) -> list[Block]:
            return self.__chain
        
        def get_serialized_chain(self) -> list[dict]:
            chain = list()
            for block in self.__chain:
                chain.append(block.serialize())
            return chain

        def descerialize_chain(self) -> None:
            self.__read_dir()
            Server.logger.info("Starting chain deserialization")
            for file in self.__files:
                file_chunk_path = os.path.join(self.__path, file)
                with open(file_chunk_path, 'r') as file:
                    chunk = json.loads(file.read())
                    self.__chunks.append(chunk)
            Server.logger.info("Reading and unfolding chunk")
            for chunk in self.__chunks:
                blocks: list[dict] = chunk.get("blocks")
                for block in blocks:
                    block_header  = Block.Header(block.get("__header"))
                    block_payload = Block.Payload(block.get("payload"))
                    block_obj = Block(header=block_header, payload=block_payload)
                    block_obj.set_block_hash(block.get("__header").get("blockHash"))
                    self.__chain.append(block_obj)    
                    
            if len(self.__chunks) == 0:
                self.__chunks.append({
                    "blocks": [],
                    "chunkFilename": f"generated-chunk-0-{datetime.datetime.now()}.json"
                })
            Server.logger.info(f"Finished descerialization --chain-entries: {len(self.__chain)} --chunk-entries: {len(self.__chunks)}")
        
        def serialize_chain(self) -> None:
            Server.logger.info("ChainManager: starting chain serialization")
            for chunk in self.__chunks:
                Server.logger.info(f"ChainManager: chunk about to be serializated --data: {chunk}")
                chunk_filename: str = chunk.get("chunkFilename")
                writeable_data = json.dumps(chunk)
                with open(f"{globals.SERIALIZED_CHAIN_DIRECTORY}/{chunk_filename}", "w+") as f:
                    bin = f.readlines()
                    if (len(writeable_data.encode(globals.ENCODING)) != len(bin)):
                        f.write(writeable_data)
                        
        def get_last_chunk(self) -> dict | None:
            if len(self.__chunks) == 0:
                return None
            return self.__chunks[-1]
                    
        def add_block(self, block: Block) -> tuple[bool, str | None]:
            hash = Server.hash_generator(block)
            block.set_block_hash(hash)
            Server.logger.info(f"ChainManager: Hash new block --result: {hash}")
            last_block = Server.chain_manager.get_last_block()
            if last_block == None:
                self.__chain.append(block)
                self.__chunks[-1].get("blocks").append(block.serialize())
                Server.logger.info(f"ChainManager: appended on the end of the last chunk --result: {hash}")
            else:
                last_block_hash = last_block.get_header().get("blockHash")
                Server.logger.info(f"ChainManager: Last block info: {last_block.serialize()}")
                Server.logger.info(f"ChainManager: Hash verification={hash==last_block_hash}\n{hash}\n{last_block_hash}")
                if block.get_last_hash() == last_block_hash:
                    if (len(json.dumps(self.__chunks[-1]).encode(globals.ENCODING)) > 5 * 1024 * 1024):
                        self.__chunks.append({
                            "blocks": [],
                            "chunkFilename": f"generated-chunk-{len(self.__chunks)}-{datetime.datetime.now()}.json"
                        })
                    self.__chain.append(block)
                    self.__chunks[-1].get("blocks").append(block.serialize())
                else:
                    return False, None         
            self.serialize_chain()
            Server.logger.info("ChainManager: Finished serializing chain")
            return True, hash
        
        def get_chunk(self, chunkName) -> dict[str, object] | None:
            for chunk in self.__chunks:
                if chunk.get("chunkFilename") == chunkName:
                    return chunk
            
        def get_last_block(self) -> Block | None:
            if (len(self.__chain) == 0):
                return None
            return self.__chain[-1]
                    
        def __read_dir(self):
            self.__files = [file for file in os.listdir(self.__path) if os.path.isfile(os.path.join(self.__path, file))]
            Server.logger.info(f"Readed serialization dir --resolution: {self.__files}")
    
    class ClientConnectionThread(threading.Thread):
        def __init__(self, connection: socket.socket, address: tuple[str, int]) -> None:
            self.__connection = connection
            self.__address = address

            super().__init__()
            
        def run(self) -> None:
            alive = True
            while alive:
                try:
                    bin = self.__connection.recv(1024)
                    if len(bin) > 2:
                        Server.logger.info(f"Received data from client --data: {bin} from {self.__address}")
                        segmented_data = bin.split(b"\n")
                        for data in segmented_data:
                            data = data.decode(globals.ENCODING)
                            message:dict = json.loads(data)
                            message_type = message.get("message_type")
                            message_data = message.get("message_data")
                            if message_type == Server.MessageType.CLOSE.value:
                                alive = False
                                self.__connection.close()
                            else:
                                Server.logger.info(f"Putting request: {message_type} on queue")
                                Server.queue.put((message_type, message_data, self.__address, self.__connection))
                except:
                    alive = False
                    self.__connection.close()
                    
    class ThreadPoolThread(threading.Thread):
        def __init__(self) -> None:
            super().__init__()
            
        def run(self) -> None:
            with ThreadPoolExecutor(max_workers=3) as executor: 
                while True:
                    msg_type, msg_data, adrrs, conn = Server.queue.get()
                    Server.logger.info(f"Queue-message arrived --type: {msg_type} from {adrrs}")
                    executor.submit(Server.consume_message, msg_type, msg_data, adrrs, conn)
                
            return super().run()
    
    logger: logging.Logger          = logging.getLogger(__name__)
    configuration: dict[str, dict]  = dict()
    hash_generator                  = BlockHashSHA256Generator()
    
    queue: Queue[tuple[int, dict, tuple[str, int], socket.socket]] = Queue()
    chain_manager: ChainManager     = ChainManager(globals.SERIALIZED_CHAIN_DIRECTORY)
    
    
    def __init__(self) -> None:
        
        Server.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler("./logs/DataLayer.log")
        handler.setFormatter(formatter)
        Server.logger.addHandler(handler)
        self.logger.info("Data-layer initialization")

        self.__read_config()
        self.chain_manager.descerialize_chain()
        
    def start(self) -> None:
        self.logger.info("Data-layer started")
        self.__setup_thread_pool()
        self.run()
    
    def run(self) -> None:
        address = (
            self.configuration["blockchain"]["data"]["ip"],
            self.configuration["blockchain"]["data"]["port"],
        )
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(address)
            sock.listen(5)
            Server.logger.info(f"Server is listening on {address}")
            
            while True:
                conn, addr = sock.accept()
                Server.logger.info(f"Accepted connection with {addr}, starting client connection thread")
                thread = Server.ClientConnectionThread(conn, addr)
                thread.start()
        
    def __read_config(self) -> None:
        with open(globals.CONFIG_FILE, 'r') as file:
            self.configuration = yaml.safe_load(file)
    
    def __setup_thread_pool(self):
        thread = self.ThreadPoolThread()
        thread.start()
        
    @staticmethod
    def consume_message(msg_type: int, msg_data: dict, address: tuple[str, int], connection: socket.socket) -> None:
        try:
            Server.logger.info(f"Consumming message from {address}")
            if msg_type == Server.MessageType.SEND_BLOCK_CHAIN_CHUNK.value:
                chunk_name = msg_data.get("chunkFilename")
                chunk = Server.chain_manager.get_chunk(chunk_name)
                if chunk:
                    connection.sendall(json.dumps({"action": msg_type, "result": chunk , "message": "Sending chunk"}).encode(globals.ENCODING))
                else:
                    connection.sendall(json.dumps({"action": msg_type, "result": False, "message": "Error chunk may not exist" }).encode(globals.ENCODING))
            
            elif msg_type == Server.MessageType.SEND_BLOCK_CHAIN.value:
                chain = Server.chain_manager.get_serialized_chain()
                
                connection.sendall(json.dumps({"action": msg_type, "result": chain , "message": "Sending chunk"}).encode(globals.ENCODING))
            
            elif msg_type == Server.MessageType.SEND_LAST_BLOCK_CHAIN_CHUNK.value:
                chunk = Server.chain_manager.get_last_chunk()
                if chunk:
                    connection.sendall(json.dumps({"action": msg_type, "result": chunk , "message": "Sending chunk"}).encode(globals.ENCODING))
                else:
                    connection.sendall(json.dumps({"action": msg_type, "result": False, "message": "Error chunk may not exist" }).encode(globals.ENCODING))
            
            
            elif msg_type == Server.MessageType.ADD_BLOCK.value:
                Server.logger.info(f"Starting process for adding new block from {address} --data: {msg_data}")
                header = Block.Header(msg_data.get("__header"))
                payload = Block.Payload(msg_data.get("payload"))
                Server.logger.info(f"New-block --header: {header} --payload: {payload}")
                result, hash = Server.chain_manager.add_block(Block(header=header, payload=payload))
                Server.logger.info(f"Block added-process --result: {result}")
                if result:
                    connection.sendall(json.dumps({"action": msg_type, "result": result, "message": "Block Accepted", "hash": hash}).encode(globals.ENCODING))
                else:
                    connection.sendall(json.dumps({"action": msg_type, "result": result, "message": "Block Rejected"}).encode(globals.ENCODING))
                
        except Exception as e:
            connection.close()
            Server.logger.error(f"An error was raided --resolution: {e}")