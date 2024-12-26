
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import math
from queue import Queue
import random
from core.transaction import HoldStakeTransaction, UploadTransaction, DownloadTransaction, Transaction, create_transaction
from core.chain.block import Block

import json
import logging
import socket
import threading
import globals
import yaml

class Server():
    class StageManager():
        def __init__(self) -> None:
            self.__block: Block
            
        def stage_new_block(self) -> None:
            
            lastBlock   = None
            lastHash    = ""
            blockNumber = 0
            
            if len(Server.blocks) != 0: 
                lastBlock       = Server.blocks[-1]
                Server.logger.info(f"Last block loaded --data: {lastBlock}")
                lastHash        = lastBlock["__header"]["blockHash"]
                lastBlockNumber = lastBlock["__header"]["blockNumber"]
                
            header = Block.Header({
                "validators": Server.validators,
                "validatorPublicKey": "public-key",
                "validatorVerificationSequence": "123456789",
                "lastBlockHash": lastHash,
                "blockNumber": lastBlockNumber + 1,
                "nextValidators": dict(),
            })
            
            payload = Block.Payload({
                "sequence": f"{lastBlockNumber}{blockNumber}",
                "transactions": list()
            })

            block = Block(header=header, payload=payload)
            self.__block = block
            Server.logger.info(f"Staged block {block.serialize()}")
        
        def add_transaction(self, transaction: Transaction) -> None:
            transaction.fee     = 1
            transaction.cost    = 1
            
            if isinstance(transaction, UploadTransaction):
                transaction.reward = 5
            
            if isinstance(transaction, DownloadTransaction):
                transaction.reward = 0
                transaction.cost = 2    
                
            if isinstance(transaction, HoldStakeTransaction):
                transaction.reward = 5
                
            transaction.confirm()
            self.__block.add_transaction(transaction)
            
        def __validate_transaction(self) -> None:
            ...
        
        def __reject_transaction(self, transaction: Transaction) -> None:
            ...
        
        def __verify_validator_headers(self) -> None:
            ...
        
        def get_validators_pool(self) -> list[HoldStakeTransaction]:
            transactions = list()
            for t in self.__block.get_transactions():
                if t.code == Transaction.TransactionCode.HOLD_STAKE:
                    transactions.append(t)
            return transactions
                
        def trigger_block_emission(self) -> None:
            ## another validation should be when there isnt yet validators or hold stake blocks of the next validation
            if (len(self.__block.get_transactions()) >= 2):
                Server.logger.info(f"Block is with the necessary transactions holded --entries: {len(self.__block.get_transactions())}")
                next_validators_pool = self.get_validators_pool()
                next_validators_pool = sorted(next_validators_pool, key=lambda obj: obj.stake, reverse=True)
                
                if (len(next_validators_pool) >= 5):
                    next_validators_pool = next_validators_pool[0:5]
                
                if (len(next_validators_pool) != 0):
                    self.__block.set_validators(list(Server.validators.keys()))
                    for validator in next_validators_pool:
                        self.__block.add_next_validators(ip=validator.port, port=validator.port, id=validator.peer_id)

                    Server.logger.info(f"Start block emission thread: --block: {self.__block.serialize()}")
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    address = (Server.configuration["blockchain"]["data"]["ip"], Server.configuration["blockchain"]["data"]["port"])
                    thread = Server.EmitBlockToDataLayerThread(block=self.__block, sock=sock, address=address)
                    thread.start()
                else:
                    Server.logger.info(f"Block emission failed because there are not enought validator to validate the next block emission")
            else:
                Server.logger.info(f"Block is not with the necessary transactions holded --entries: {len(self.__block.get_transactions())}")
    
    class MessageType(Enum):
            CLOSE=-1
            HOLD_STAKE_TRANSACTION=1
            UPLOAD_TRANSACATION=2
            DOWNLOAD_TRANSACTION=3
            
    class ThreadCurrentBlockValidator(threading.Thread):
            def __init__(self) -> None:
                self.__peer_id = Server.configuration['service']['identity-uuid']
                super().__init__()
                
            def run(self) -> None: 
                Server.logger.info(f"Getting last block validator --blocks: {Server.blocks}")
                if (len(Server.blocks) == 0):
                    ## logic for when its empty
                    ## we will contact every peer asking for the current validator, 
                    ## if a validator exists
                    ## Send a message to the network layer to ask for a copy of the blockchaib
                    ## if a validator do not exists
                    ## connect with all possible pears and decide by a consensus who will be the 3 current validators
                    ## NI
                    pass
                else:
                    block = Server.blocks[-1]
                    Server.validators = block.get("__header")["nextValidators"]
                    Server.logger.info(f"Next validators are: {Server.validators} on block: {block}")
                    keys = list(Server.validators.keys())
                    is_validator = self.__peer_id in keys
                    Server.logger.info(f"--is_validator: {is_validator} --peer_id: {self.__peer_id} --keys: {keys}")
                    
                    if self.__peer_id in list(Server.validators.keys()):
                        Server.is_validador = True
                        Server.stage_manager.stage_new_block()
            
    class ThreadPool(threading.Thread):
        def __init__(self) -> None:
            super().__init__()
        
        def run(self) -> None:
            while True:
                msg_type, msg_data, address, conn = Server.queue.get()
                thread = threading.Thread(target=Server.consume_message, args=(msg_type, msg_data, address, conn))
                thread.start()

    class RequestBlockchainChunk(threading.Thread):
        def __init__(self) -> None:
            super().__init__()
            self.__data_address = (Server.configuration["blockchain"]["data"]["ip"], Server.configuration["blockchain"]["data"]["port"])
            self.__encoded_message_data = json.dumps({"message_type": 4}).encode(globals.ENCODING)
            
        def run(self) -> None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            Server.logger.info(f"Attemping socket connection with --address: {self.__data_address}")
            sock.connect(self.__data_address)
            sock.sendall(self.__encoded_message_data)
            bin = sock.recv(4024)
            Server.logger.info(f"Received from blockchain-chunk request: {bin}")
            data = json.loads(bin.decode(globals.ENCODING))
            Server.blocks = data['result']['blocks']
            return super().run()
            
    class LocalThreadConnection(threading.Thread):
        def __init__(self, connection: socket.socket, address: tuple[str, int]) -> None:
            self.__conn = connection
            self.__address = address
            super().__init__()
            
        def run(self) -> None:
            alive = True
            while alive:
                bin = self.__conn.recv(1024)
                if len(bin) > 2:
                    Server.logger.info(f"Received {bin} from {self.__conn}")
                    segmented_data = bin.split(b"\n")
                    for data in segmented_data:
                        data = data.decode(globals.ENCODING)
                        message: dict = json.loads(data)
                        message_type = message.get("message_type")
                        message_data = message.get("message_data")
                            
                        if message_type == Server.MessageType.CLOSE.value:
                            alive = False
                            self.__conn.close()
                        else:
                            Server.queue.put((message_type, message_data, self.__address, self.__conn))
            self.__conn.close()
    
    class EmitBlockToDataLayerThread(threading.Thread):
        def __init__(self, block: Block, sock: socket.socket, address: tuple[str, int]) -> None:
            self.__block    = block
            self.__sock     = sock
            self.__address  = address
            self.__sock.connect(self.__address)
            super().__init__()
        
        def run(self) -> None:
            self.__sock.sendall(json.dumps({
                "message_type": 3,
                "message_data": self.__block.serialize()
            }).encode(globals.ENCODING))
            Server.logger.info("Connected with data layer and sended serialized data")
            bin = self.__sock.recv(1024)
            data = json.loads(bin.decode(globals.ENCODING))
            Server.logger.info(f"Emitted block response --data: {data}")
            self.__sock.close()
            return super().run()
                
    logger: logging.Logger                                          = logging.getLogger(__name__)
    configuration: dict[str, object]                                = dict()
    blocks: list[dict[str, dict]]                                   = list()
    queue: Queue[tuple[int, dict, tuple[str, int], socket.socket]]  = Queue()
    is_validador: bool                                              = False
    validators: dict[str, dict]                                     = dict()
    stage_manager                                                   = StageManager()
    local_hostnames                                                 = ["127.0.0.1", socket.gethostbyname(socket.gethostname())]
    
    
    def __init__(self) -> None:
        Server.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler("./logs/ConsensusLayer.log")
        handler.setFormatter(formatter)
        Server.logger.addHandler(handler)
        self.logger.info("ConsensusServer initialization")
        self.__read_config()
        
    def start(self) -> None:
        self.logger.info("ConsensusServer Started")
        thread_data = Server.RequestBlockchainChunk()
        thread_pool = Server.ThreadPool()
        thread_validator = Server.ThreadCurrentBlockValidator()
        thread_data.start()
        thread_data.join()
        thread_validator.start()
        thread_pool.start()
        
        self.run()

    def run(self) -> None:
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((
                Server.configuration["blockchain"]["consensus"]["ip"],
                Server.configuration["blockchain"]["consensus"]["port"]
            ))
            sock.listen(3)
            
            try:
                while True:
                    conn, addr = sock.accept()
                    self.logger.info(f"Connection from {addr}")
                    client_thread = Server.LocalThreadConnection(conn, addr)
                    client_thread.start()
            except Exception as e:
                Server.logger.error(f"Error when handling connection: {e}")
                conn.close()
                
    def __read_config(self):
        with open(globals.CONFIG_FILE, 'r') as file:
            Server.configuration = yaml.safe_load(file)
    
    @staticmethod
    def consume_message(msg_type: int, msg_data: dict, address: tuple[str, int], connection: socket.socket):
        Server.logger.info(f"Consuming message from {address}")
        if Server.is_validador:
            transaction = create_transaction(msg_type, msg_data)
            Server.logger.info(f"Adding transaction: {transaction}")
            Server.stage_manager.add_transaction(transaction)
            Server.stage_manager.trigger_block_emission()
        else:
            # if a not a validator and a transaction its comming should be local and i have to repass to the current validators
            Server.logger.info(f"Forward transaction to {Server.validators}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            for key in Server.validators:
                validator_address = (Server.validators[key]["ip"], Server.validators[key]["port"])
                Server.logger.info(f"Forward transaction to peer {key} in {validator_address}")
                sock.connect(validator_address)
                sock.sendall(json.dumps({
                        "message_type": msg_type,
                        "message_data": msg_data 
                }).encode(globals.ENCODING))
        connection.sendall(json.dumps({"response": "Transaction Submitted"}).encode(globals.ENCODING))
                
            
            
            
            