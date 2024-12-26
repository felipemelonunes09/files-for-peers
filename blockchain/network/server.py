
from concurrent.futures import ThreadPoolExecutor
from typing import Generator
from enum import Enum
import json
import logging
import math
import pickle
import queue
import socket
import threading
import yaml
import globals

class Server():
    class FBE():
        def __init__(self) -> None:
            self.__queue: queue.Queue[dict] = queue.Queue()
            self.__range: int = 50 # peers per thread
            
        def stage_size(self) -> int:
            return len(self.__queue)
        
        def get_workeable_range(self) -> Generator:
            hashtable_keys = list(Server.user_hashtable.keys())
            hashtable_len = len(Server.user_hashtable)
            chunks = math.ceil(hashtable_len/self.__range)
            
            for i in range(0, chunks):
                start_index = i*self.__range
                end_index = (i*self.__range) + self.__range
                
                if (end_index < hashtable_len):
                    yield hashtable_keys[start_index: end_index]
                else:
                    yield hashtable_keys[start_index:]

        def stage_block(self, block: dict) -> None:
            self.__queue.put(block)
            
        def start_pool(self) -> None:
            Server.logger.info("FBE: starting thread pool executor")
            for keys_set in self.get_workeable_range():
                thread = threading.Thread(target=self.__emit_block, args=(keys_set,))
                thread.start()
                
        def __add_header(self) -> None: # NI
            ...
        
        def __sign_header(self) -> None: # NI
            ...
        
        def __emit_block(self, keys: list[str]) -> None: 
            Server.logger.info(f"FBE: Starting block emission with {len(keys)} entries --queue-empty: {self.__queue.empty()}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            while not self.__queue.empty():
                block = self.__queue.get() 
                Server.logger.info(f"FBE: Next emitted block --data: {block}")
                for key in keys:
                    Server.logger.info(f"FBE: Next key to be sent --data: {key}")
                    peer = Server.user_hashtable.get(key)
                    sock.connect((peer["ip"], int(peer["ports"]["networkLayer"])))
                    Server.logger.info(f"Sending request of incoming block to {peer["ip"]}")
                    sock.sendall(json.dumps({
                        "request_type": Server.MessageType.INCOMING_BLOCK,
                        "request_data": block
                    }).encode(globals.ENCODING))
                    
            sock.close()
    
    class MessageType(Enum):
        INCOMING_BLOCK=1
        BLOCKCHAIN_REQUEST=2
        
    class ClientResponse():
        class OPERATION_CODE(Enum):
            WAITING_VALIDATION                  = 0
            ACCEPTED_AND_FORWARD                = 1 
            REJECTED_INVALID_PUBLIC_KEY         = -1 # NI
            REJECTED_INVALID_INVALID_SEQUENCE   = -2 # NI
            REJECTED_INVALID_BLOCK_HASH         = -3 # NI
            REJECTED_INVALID_CHAIN_CONNECTION   = -4
            REJECTED_INVALID_BLOCK_NUMBER       = -5
            REJECTED_INVALID_VALIDATOR          = -6 # NI
            REJECTED_DATA_LAYER_NOT_ACCEPTED    = -7
            REJECTED_INVALID_FBE_HEADER         = -8 # NI
            SENDING_SERIALIZED_BLOCKAIN         = 2
            
            
        def __init__(self, opt_code: OPERATION_CODE=None, msg: str=None) -> None:
            self.set(opt_code, msg)
            
        def set(self, opt_code: OPERATION_CODE, msg: str, payload: object = None) -> None:
            self.opt_code = opt_code
            self.msg = msg
            self.payload = payload
        
            
        def serialize(self) -> bytes:
            return json.dumps({ "opt_code": self.opt_code.value, "msg": self.msg, "payload": self.payload }).encode(globals.ENCODING)
        
    class RequestDataThread(threading.Thread):
        def __init__(self) -> None:
            self.__encoded_message_udht = json.dumps({ "message_type": 2 }).encode(globals.ENCODING)
            self.__encoded_message_data = json.dumps({ "message_type": 4 }).encode(globals.ENCODING)
            self.__udht_address = (Server.configuration["udht"]["manager"]["ip"], Server.configuration["udht"]["manager"]["port"])
            self.__data_address = (Server.configuration["blockchain"]["data"]["ip"], Server.configuration["blockchain"]["data"]["port"])
            super().__init__()
            
        def run(self) -> None:
            self.__request_udht()
            self.__request_blockchain_chunk()
            return super().run()
        
        def __request_udht(self) -> None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(self.__udht_address)
            sock.sendall(self.__encoded_message_udht)
            bin = sock.recv(4024)
            Server.logger.info(f"{self}: Received user hashtable --data: {bin}")
            hashtable = pickle.loads(bin)
            sock.close()
            Server.user_hashtable = hashtable
        
        def __request_blockchain_chunk(self) -> None:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.__data_address)
                sock.sendall(self.__encoded_message_data)
                bin = sock.recv(4028)
                data = bin.decode(globals.ENCODING)
                Server.logger.info(f"{self}: Received decoded data chunk --data: {data}")
                data: dict = json.loads(data)
                Server.blocks = data["result"]["blocks"]
            
    class ClientThreadConnection(threading.Thread):
        def __init__(self, conn: socket.socket, address: tuple[str, int]) -> None:
            self.__conn = conn
            self.__address = address
            super().__init__()
        
        def run(self) -> None:
            arr_bytes = self.__conn.recv(1024)          
            data:dict = json.loads(arr_bytes.decode(globals.ENCODING))  
            Server.logger.info(f"ClientThreadConnection:{self.__address}: incoming request --data: {data}")
            request_type = data.get("request_type")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(Server.data_layer_adress)
            
            # Incoming block
            if request_type == Server.MessageType.INCOMING_BLOCK.value:
                response = Server.ClientResponse(opt_code=Server.ClientResponse.OPERATION_CODE.WAITING_VALIDATION)
                request_data = data.get("request_data")
                block_header: dict = request_data["__header"]
                add_block_message = { "message_type": 3, "message_data": request_data }
                data_response: bytes = None
                
                #  should validate with the public key of the validator
                # ---------------------------------------------------------
                
                if (len(Server.blocks) == 0):
                    sock.sendall(json.dumps(add_block_message).encode(globals.ENCODING))
                    data_response = sock.recv(1024)
                else:
                    current_block:dict = Server.blocks[-1]
                    Server.logger.info(f"ClientThreadConnection: resolver: there are loaded blocks in the chunk --lastBlock: {current_block}")
                    
                    ## verify chain connection
                    current_hash = current_block.get("__header")["blockHash"]
                    incoming_block_hash = block_header.get("lastHash")
                    
                    if (current_hash != incoming_block_hash):
                        response.set(Server.ClientResponse.OPERATION_CODE.REJECTED_INVALID_CHAIN_CONNECTION, "Incoming block hash dit not connect with block chain")
                     
                    ## verify block number
                    current_block_number = current_block.get("__header")["blockNumber"]
                    incoming_block_number = block_header.get("blockNumber")
                    
                    # should verify FBE header
                    # ---------------------------------------------------------
                    
                    if (incoming_block_number != current_block_number + 1):
                        response.set(Server.ClientResponse.OPERATION_CODE.REJECTED_INVALID_BLOCK_NUMBER, "Incoming block number dit not had a valid block number")

                    if (response.opt_code == Server.ClientResponse.OPERATION_CODE.WAITING_VALIDATION):
                        sock.sendall(json.dumps(add_block_message).encode(globals.ENCODING))
                        data_response = sock.recv(1024)
                
                if data_response != None:
                    decoded_data = data_response.decode(globals.ENCODING)
                    Server.logger.info(f"ClientThreadConnection: data-layer response --data: {decoded_data}")
                    data: dict = json.loads(decoded_data)
                    result = data.get("result")
                    hash = data.get("hash")
                    response.opt_code = Server.ClientResponse.OPERATION_CODE.ACCEPTED_AND_FORWARD if result else Server.ClientResponse.OPERATION_CODE.REJECTED_DATA_LAYER_NOT_ACCEPTED
                    response.msg = data.get("message")
                
                Server.logger.info(f"ClientThreadConnection: response to {self.__address}: {response.serialize()}")
                self.__conn.sendall(response.serialize())
                
                # If accepted send it to the Forward Block Enhenment
                if response.opt_code == Server.ClientResponse.OPERATION_CODE.ACCEPTED_AND_FORWARD:
                    Server.forward_block_enhencement.stage_block(request_data)
                    request_data["__header"]["blockHash"] = hash
                    Server.blocks.append(request_data)
                    Server.forward_block_enhencement.start_pool()
                    
            # Incoming blockchain request or chunk request
            if request_type == Server.MessageType.BLOCKCHAIN_REQUEST.value:
                sock.sendall(json.dumps({ "message_type": 2}).encode(globals.ENCODING))
                bin = sock.recv(1024)
                data = json.loads(bin.decode(globals.ENCODING))
                response = Server.ClientResponse()
                response.set(Server.ClientResponse.OPERATION_CODE.SENDING_SERIALIZED_BLOCKAIN, "Sending blockchain", payload=data)
                self.__conn.sendall(response.serialize())
                
            sock.close()
            self.__conn.close()
    
    logger: logging.Logger              = logging.getLogger(__name__)
    configuration: dict[str, object]    = dict()
    user_hashtable: dict[str, dict]     = dict()
    blocks: list[dict[str, dict]]       = dict()
    data_layer_adress: tuple[str, int]  = tuple()
    forward_block_enhencement           = FBE()
    
    def __init__(self) -> None:
        Server.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler("./logs/NetworkLayer.log")
        handler.setFormatter(formatter)
        Server.logger.addHandler(handler)
        self.logger.info("NetworkLayer initialization")
        self.__read_config()
        Server.data_layer_adress = (Server.configuration["blockchain"]["data"]["ip"], Server.configuration["blockchain"]["data"]["port"])
        
    def start(self) -> None:
        self.logger.info("NetworkLayer started")
        self.__setup_user_hashtable()
        self.run()
    
    def __setup_user_hashtable(self) -> None:
        thread = Server.RequestDataThread()
        thread.start()
        thread.join()
    
    def run(self) -> None:
        address = (
            Server.configuration["blockchain"]["network"]["ip"],
            Server.configuration["blockchain"]["network"]["port"],
        )

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(address)
            sock.listen(50)  # Pool de no máximo 50 conexões ativas
            Server.logger.info(f"Network server is listening on {address}")
            
            while True:
                conn, addr = sock.accept()
                Server.logger.info(f"Accepted connection from {addr}, starting client connection thread")
                thread = Server.ClientThreadConnection(conn, addr)
                thread.start()
    
    def __read_config(self) -> None:
        with open(globals.CONFIG_FILE, 'r') as file:
            Server.configuration = yaml.safe_load(file)
    