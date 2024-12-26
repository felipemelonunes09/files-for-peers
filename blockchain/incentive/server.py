
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import pandas
import json
import logging
import socket
import threading
import yaml
import globals

class Server():
    class ServerMessage(Enum):
        CALCULATE_PEER_CURRENCY      = 1
        CALCULATE_TRANSACTION_FEE    = 2
        CALCULATE_TRANSACTION_REWARD = 3
        CALCULATE_TRANSACTION_COST   = 4
        CALCULATE_PEER_TOKENS_HASH   = 5
    
    logger          : logging.Logger              = logging.getLogger(__name__)
    configuration   : dict[str, object]           = dict()
    transactions    : pandas.DataFrame            = pandas.DataFrame()
    
    class RequestBlockchainThread(threading.Thread):
        def __init__(self, connection: socket.socket, address: tuple[str, int]) -> None:
            self.connection = connection
            self.address = address
            self.connection.connect(address)
            super().__init__()
            
        def run(self) -> None:
            self.connection.sendall(json.dumps({ "message_type": 2 }).encode(globals.ENCODING))
            bin = self.connection.recv(4024)
            data = json.loads(bin.decode(globals.ENCODING))
            chain = data["result"]
            transactions = [tx for block in chain for tx in block["payload"]["transactions"]]
            Server.logger.info(f"Blockchain transactions --data: {transactions}")
            df = pandas.DataFrame(transactions)
            Server.transactions = df
            Server.logger.info(f"--len: {len(df)}")
            Server.logger.info(df)
            Server.logger.info(df["peerId"])
            return super().run()
        
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
                        Server.logger.info(f"Received {bin} from {self.__address}")
                        data = json.loads(bin.decode(globals.ENCODING))
                        message_type = int(data["message_type"])
                        message_data = data["message_data"]
                        
                        if message_type == Server.ServerMessage.CALCULATE_PEER_CURRENCY.value:          ## Not fixed
                            Server.logger.info(f"Calculating peer currency for chain --len: {len(Server.transactions)}")
                            currency = 0
                            if len(Server.transactions) != 0:
                                peer_id = message_data["peer_id"]
                                Server.logger.info(f"Requesting currency for {peer_id}")
                                filtered_transaction: pandas.DataFrame = Server.transactions.loc[
                                    (Server.transactions["peerId"] == peer_id) 
                                ]
                                Server.logger.info(f"Peer transactions {filtered_transaction}")
                                initial_value   = 5
                                total_cost      = filtered_transaction.loc[filtered_transaction["transactionCost"].notna(), "transactionCost"].sum()
                                total_reward    = filtered_transaction.loc[filtered_transaction["transactionReward"].notna(), "transactionReward"].sum() 
                                total_fee       = filtered_transaction.loc[filtered_transaction["transactionFee"].notna(), "transactionFee"].sum()
                                Server.logger.info(f"--initial-value: {initial_value} --total-cost: {total_cost} --total-reward: {total_reward}")
                                currency = initial_value + total_reward - (total_cost + total_fee)
                            else:
                                Server.logger.info("There are no transactions for calculating currency")
                            self.__connection.sendall(str(currency).encode(globals.ENCODING))
                        if message_type == Server.ServerMessage.CALCULATE_PEER_TOKENS_HASH.value:        ## NI
                            pass
                        if message_type == Server.ServerMessage.CALCULATE_TRANSACTION_REWARD.value:      ## Fixed
                            self.__connection.sendall(str(5).encode(globals.ENCODING))
                        if message_type == Server.ServerMessage.CALCULATE_TRANSACTION_COST.value:        ## Fixed
                            self.__connection.sendall(str(1).encode(globals.ENCODING))
                        if message_type == Server.ServerMessage.CALCULATE_TRANSACTION_FEE.value:         ## Fixed
                            self.__connection.sendall(str(1).encode(globals.ENCODING))
                except Exception as e:
                    alive = False
                    self.__connection.close()
                    Server.logger.error(f"Error when handling: {self.__address} --err: {e}")
            return super().run()
    
    def __init__(self) -> None:
        Server.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler("./logs/IncetiveLayer.log")
        handler.setFormatter(formatter)
        Server.logger.addHandler(handler)
        self.logger.info("Incentive initialization")
        self.__read_config()
        
    def start(self) -> None:
        self.logger.info("IncentiveLayer started")
        thread = Server.RequestBlockchainThread(socket.socket(socket.AF_INET, socket.SOCK_STREAM), address=(Server.configuration["blockchain"]["data"]["ip"], Server.configuration["blockchain"]["data"]["port"]))
        thread.start()
        self.run()
    
    def run(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((Server.configuration["blockchain"]["incentive"]["ip"], Server.configuration["blockchain"]["incentive"]["port"]))
            sock.listen(3)
            
            while True:
                conn, addr = sock.accept()
                thread = Server.ClientConnectionThread(connection=conn, address=addr)
                thread.start()
            
    def __read_config(self) -> None:
        with open(globals.CONFIG_FILE, 'r') as file:
            Server.configuration = yaml.safe_load(file)
    