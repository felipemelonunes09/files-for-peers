import getopt
import json
import queue
import socket
import globals
import yaml

class Server():
    
    configuration   : dict[str, object]           = dict()

    def __init__(self) -> None:
        self.close_message = json.dumps({"message_type": 1}).encode(globals.ENCODING)
        self.__read_config()
        
    def __read_config(self) -> None:
        with open(globals.CONFIG_FILE, 'r') as file:
            Server.configuration = yaml.safe_load(file)
    
    def run(self, command: str):
                
        is_transaction      = bool(False)
        is_consult          = bool(False)
        consult_code        = bool(0)
        transaction_code    = int(0)
        peer_port           = int(0) 
        stake               = int(0)
        peer_id             = str(None)
        peer_ip             = str(None)
        filename            = str(None)
        receiver            = str(None)
        address             = tuple([str(), int()])
        message             = dict()
    
        opts, args = getopt.getopt(command, "r:", ["operation=", "id=", "stake=","ip=", "port=", "filename=", "receiver="])
        
        for option, argument in opts:
            if option == "--operation":
                if argument == "transaction":
                    is_transaction = True
                    address = (Server.configuration["blockchain"]["consensus"]["ip"], Server.configuration["blockchain"]["consensus"]["port"])
                    print("[Server] Creating a transaction process")
                elif argument == "consult":
                    is_consult = True
                    address = (Server.configuration["blockchain"]["incentive"]["ip"], Server.configuration["blockchain"]["incentive"]["port"])
                    print("[Server] Creating a consulting process")
                    
            if option == "-r":
                if argument == "stake":
                    transaction_code = 1
                elif argument == "upload":
                    transaction_code = 2
                elif argument == "download":
                    transaction_code = 3
                elif argument == "currency":
                    consult_code = 1
                elif argument == "fee":
                    consult_code = 2
                elif argument == "reward":
                    consult_code = 3
                elif argument == "cost":
                    consult_code = 4
                elif argument == "token":
                    consult_code = 5
                
            if option == "--id":
                peer_id = argument
            
            if option == "--ip":
                peer_ip = argument
                
            if option == "--port":
                peer_port = argument
                
            if option == "--filename":
                filename = argument
            
            if option == "--receiver":
                receiver = argument
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f"[Conn 1] Address connection is {address}")
        sock.connect(address)
        if is_transaction:
            print(f"(+) Creating a transation --code={transaction_code}")
            message = {
                "message_type": transaction_code,
                "message_data": {
                    "filename": filename,
                    "receiver": receiver,
                    "peer_id": peer_id,
                    "stake": stake,
                    "port": peer_port,
                    "ip": peer_ip
                }
            }
            
        elif is_consult:
            print(f"(+) Consulting --code={transaction_code}")
            message = {
                "message_type": consult_code,
                "message_data": {
                    "peer_id": peer_id
                }
            }
        
        sock.sendall(json.dumps(message).encode(globals.ENCODING))
        response = sock.recv(4024)
        response = json.loads(response.decode(globals.ENCODING))
        
        print(json.dumps(response, indent=4))
        sock.sendall(self.close_message)
        sock.close()