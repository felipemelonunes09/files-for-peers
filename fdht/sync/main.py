from apscheduler.schedulers.background import BackgroundScheduler
from abc import ABC, abstractmethod
from typing import Any, Type
import requests
import datetime
import logging
import threading
import globals
import yaml
import socket
import pickle
import json

class HashTableConnection(ABC):
    def __init__(self, address: tuple[str, int], *args, **kwd) -> None:
        self.__address=address
        super().__init__()
    
    def get_adress(self) -> tuple[str, int]:
        return self.__address
    
    @abstractmethod
    def send_hashtable_entry(self, entry: dict) -> None:
        pass
            
    @abstractmethod
    def receive_hashtable(self, payload: Any) -> dict[str, dict]:
        pass

## This class refers not with a connection with the peer but a connnection 
## with the microservice to load the user hashtable
class TCPHashtableConnection(HashTableConnection):
    def __init__(self, address: tuple[str, int], encoding: str = globals.ENCODING) -> None:
        self.__sock                 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__address              = address
        self.__encoding             = encoding
        self.__keep_alive           = False
        self.__connected            = False
        self.__entry_payload        = { "message_type": 3, "data": {} }
        self.__hashtable_payload    = { "message_type": 2 }
        self.__close_payload        = { "message_type": 1 }
        
    def receive_hashtable(self) -> dict[str, dict]:
        if self.__connected == False:
            Server.logger.info(f'Attemping tcp-connection to: {self.__address}')
            self.__sock.connect(self.__address)
            self.__connected = True
        Server.logger.info(f'Sending {self.__hashtable_payload} with tcp-connection to: {self.__address} with encoding {self.__encoding}')
        self.__sock.sendall(json.dumps(self.__hashtable_payload).encode(self.__encoding))
        hashtable = self.__sock.recv(1024)
        if self.__keep_alive == False and self.__connected:
            self.close()
        return pickle.loads(hashtable)
    
    def send_hashtable_entry(self, entry: dict) -> None:
        if self.__connected == False:
            Server.logger.info(f'Attemping tcp-connection to: {self.__address}')
            self.__sock.connect(self.__address)
            self.__connected = True
        self.__entry_payload['data'] = entry
        data = json.dumps(self.__entry_payload).encode(self.__encoding)
        Server.logger.info(f'Sending {data} with tcp-connection to: {self.__address} with encoding {self.__encoding}')
        self.__sock.send(data + b'\n')
        response = self.__sock.recv(1024)
        Server.logger.info(f'Received {response} from service --resolution: finish method')
        if self.__keep_alive == False and self.__connected:
            self.close()
            
    def set_keep_alive(self, keep: bool) -> None:
        self.__keep_alive = keep
        
    def close(self) -> None:
        Server.logger.info(f'Attemping to close tcp-connection: {self.__address}')
        if self.__connected:
            self.__sock.sendall(json.dumps(self.__close_payload).encode(self.__encoding))
            self.__sock.close()
            self.__connected = False
            
class RESTHashtableConnection(HashTableConnection):
    def __init__(self, address: tuple[str, int], *args, **kwd) -> None:
        self.__url = f"http://{address[0]}:{address[1]}/hashtable"
        super().__init__(address, *args, **kwd)
        
    def receive_hashtable(self, payload: Any=None) -> dict[str, dict]:
        Server.logger.info(f"Making request to {self.__url}")
        response = requests.get(self.__url)
        response.raise_for_status()
        response = response.json()
        return response
    
    def send_hashtable_entry(self, entry: dict) -> None:
        try:
            Server.logger.info(f"Sending {entry} with REST-Connection to {self.__url}")
            response = requests.post(self.__url, json=entry)
            Server.logger.info(f"Server response: {response.json()}")
            response.raise_for_status()
        except Exception as e:
            Server.logger.error(f"Unexpected error when sending request --resolution: {e}")

class Server():
    class PeerSyncJob():
        def __init__(self, address) -> None:
            connection=TCPHashtableConnection(address)
            self.__hashtable = connection.receive_hashtable()
        
        def __call__(self) -> Any:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            hashtable = self.__hashtable
            for key in hashtable:
                peer_ip = hashtable[key].get("ip", None)
                peer_port = hashtable[key].get("port", None)
                sock.connect((peer_ip, int(peer_port)))
                sock.send(json.dumps(hashtable).encode(globals.ENCODING))
                peer_hashtable = sock.recv(1024)
                peer_hashtable = json.loads(peer_hashtable.decode(globals.ENCODING))
                Server.merge_hashtables(peer_hashtable)
                
        
    class TableSyncJob():
        def __init__(self, connection: HashTableConnection) -> None:
            self.__connection = connection
            Server.logger.info(f"Setting connection --resolution: set_keep_alive={isinstance(connection, TCPHashtableConnection)}")
            if isinstance(connection, TCPHashtableConnection):
                connection.set_keep_alive(True)
                
        def get_connection(self) -> HashTableConnection:
            return self.__connection
            
        def __call__(self, hashtable: dict[str, dict]) -> Any:
            Server.logger.info("Starting table sync job --resolution: peer-loop")
            connection = self.get_connection()
            Server.logger.info(hashtable)
            for key in hashtable:
                try:
                    Server.logger.info(f"Hashtable key about to be send: --resolution: {key}")
                    connection.send_hashtable_entry(hashtable.get(key))
                except Exception as e:
                    Server.logger.error(f"An error was raised when sending entry: --resolution: {e} --entry: {hashtable.get(key)}")
            
            if isinstance(connection, TCPHashtableConnection):
                connection.close()

    class ConnectionPool():
        class ConnectionThread(threading.Thread):
            def __init__(self, connection: socket.socket, address: tuple[str, int], hashtable: dict[str, dict]) -> None:
                self.__connection = connection
                self.__adress = address
                self.hashtable = hashtable
                super().__init__()
            
            def get_adress(self) -> tuple[str, int]:
                return self.__adress
            
            def get_connection(self) -> socket.socket:
                return self.__connection
            
            def run(self) -> None:
                Server.ConnectionPool.remove_thread(id(self))
            
        class ClientConnectionThread(ConnectionThread):
            def run(self) -> None:
                Server.logger.info(f"Starting client connection thread sync with: {self.get_adress()}")
                conn = self.get_connection()
                data = conn.recv(1024)
                decoded_hashtable = json.loads(data.decode(globals.ENCODING))
                encoded_hashtable = json.dumps(self.hashtable).encode(globals.ENCODING)
                conn.sendall(encoded_hashtable)
                Server.merge_hashtables(decoded_hashtable)
                Server.logger.warning(f"Finished hashtable merge --hashtable not persisted await for the next job schedule")
                return super().run()
            
        class ServerConnectionThread(ConnectionThread):
            def run(self) -> None:
                return super().run()
            
        __pool: set[int] = set()
        __lock: threading.Lock = threading.Lock()
        __limit: int = globals.THREAD_POOL_LIMIT
            
        def add_connection_thread(self, thread: ConnectionThread) -> bool:
            with self.__lock:
                if len(self.__pool) < self.__limit:
                    self.__pool.add(id(thread))
                    thread.start()
                    return True
                return False
        
        @staticmethod
        def remove_thread(id: str) -> None:
            with Server.ConnectionPool.__lock:
                Server.ConnectionPool.__pool.remove(id)
    
    class DHTThreadRequest(threading.Thread):
        def __init__(self, connection: HashTableConnection):
            self.__connection = connection
            super().__init__()
            
        def get_connection(self) -> HashTableConnection:
            return self.__connection
        
        def run(self) -> None:
            try:
                Server.logger.info('RequestUDHTThread started')
                connection = self.get_connection()
                hashtable = connection.receive_hashtable()
                Server.hashtable = hashtable
                Server.logger.info('RequestUDHTThread finished --resolution: builded hashtable and closing state')
                Server.logger.info(f'User Hash Table entries {len(Server.hashtable)}')
                super().run()
            except ConnectionRefusedError as e:
                Server.logger.error(f'RequestUDHTThread finished --resolution: connection-refused: {e}')
            except ConnectionError as e:
                Server.logger.error(f'RequestUDHTThread finished --resolution: connection-error: {e}')
            except Exception as e:
                Server.logger.error(f"Unexpected error --resolution: {e}")
    
    hashtable: dict[str, dict]      = dict()
    configuration: dict[str, dict]  = dict()
    changes: dict[str, dict]        = dict()
    logger: logging.Logger          = logging.getLogger(__name__)
    thread_pool: ConnectionPool     = ConnectionPool()
    diff_count: int                 = 0
    changes: set[str]               = set()
    
    TCPHashtableConn: TCPHashtableConnection   = TCPHashtableConnection
    RESTHashtableConn: RESTHashtableConnection = RESTHashtableConnection 
    
    ## should segregate into another file and use dependency injection
    scheduler: BackgroundScheduler  = BackgroundScheduler()

    def __init__(self) -> None:
        Server.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler(globals.LOG_NAME)
        handler.setFormatter(formatter)
        Server.logger.addHandler(handler)
    
    def start(self) -> None:
        self.logger.info('Server started --resolution: \n(+)\tread-config\n(+)\tsetup-hashtable\n(+)\tsetup-jobs')
        self.__read_config()
        self.__setup_hashtable()
        self.__setup_jobs()
        self.run()
    
    def run(self) -> None:
        self.logger.info('Server running on listen mode --resolution:\n(+)\t listening for peers')
        connection  = (self.configuration['fdht']['sync']['ip'], self.configuration['fdht']['sync']['port'])
        pool_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        pool_socket.bind(connection)
        pool_socket.listen(globals.SOCKET_CONNECTION_LIMIT)
        
        while True:
            in_connection, in_address = pool_socket.accept()
            self.logger.info(f'Incoming connection from {in_address} --resolution: creating client connect thread and adding to the pool')
            client_thread = Server.ConnectionPool.ClientConnectionThread(in_connection, in_address, Server.hashtable)
            self.thread_pool.add_connection_thread(client_thread)
    
    def __setup_hashtable(self) -> None:
        thread = Server.DHTThreadRequest(connection=self.get_service_connection()(self.get_connection_tuple()))
        thread.start()
        thread.join()
    
    def __setup_jobs(self) -> None:
        connection = self.get_service_connection()
        self.scheduler.add_job(self.TableSyncJob(connection=connection(self.get_connection_tuple())), 'interval', seconds=globals.SCHEDULER_TABLE_SYNC_JOB_HOUR_INTERVAL, args=[Server.hashtable])
        self.scheduler.add_job(self.PeerSyncJob(address=(self.configuration['udht']['manager']['ip'], self.configuration['udht']['manager']['port'])), 'interval', seconds=globals.SCHEDULER_PEER_SYNC_JOB_HOUR_INTERVAL)
        self.scheduler.start()
        Server.logger.info("Sheduler started --resolution: \n(+)\t awaiting for TableSyncJob\n(+)\t awaiting for PeerSyncJob")
    
    def __read_config(self) -> None:
        with open(globals.CONFIG_FILE, 'r') as file:
            self.configuration = yaml.safe_load(file)
            
    def get_connection_tuple(self) -> tuple[str, int]:
        service_name = self.configuration.get("service").get("name")
        service_name = service_name.split(":")
        return (self.configuration[service_name[0]]['manager']['ip'], self.configuration[service_name[0]]['manager']['port'])
    
    def get_service_connection(self) -> HashTableConnection:
        connection_atrr = self.configuration.get('service').get('connection')
        connection_class: Type[HashTableConnection] = getattr(self, connection_atrr)
        return connection_class
            
    @staticmethod
    def merge_hashtables(peer_hashtable: dict[str, dict]) -> None:
        unique_keys   = set(peer_hashtable)-set(Server.hashtable)
        conflict_keys = set(peer_hashtable.keys()) & set(Server.hashtable.keys())
        Server.logger.info(f"Peer diff: {len(unique_keys)} entries")
        Server.logger.info(f"Peer conflict: {len(conflict_keys)} entries")
        for key in unique_keys:
            Server.hashtable[key] = peer_hashtable[key]
            Server.diff_count += 1
            Server.logger.info(f"Added peer {key} to in-memory hashtable --diff: {Server.diff_count}")
            
        for key in conflict_keys:
            Server.logger.info(f"Peer conflict: {key} --resolution: 002.1")
            client_updated_at = peer_hashtable.get(key).get('updatedAt')
            server_updated_at = Server.hashtable.get(key).get('updatedAt')
            client_updated_at = datetime.datetime.strptime(client_updated_at, '%Y-%m-%d %H:%M:%S.%f')
            server_updated_at = datetime.datetime.strptime(server_updated_at, '%Y-%m-%d %H:%M:%S.%f')
            
            if client_updated_at > server_updated_at: 
                Server.diff_count += 1
                Server.logger.info(f"Updated peer {key} to in-memory hashtable --diff: {Server.diff_count}")
                Server.hashtable[key] = peer_hashtable[key]
                Server.changes.add(key)
        
        Server.logger.info(f"Finished hashtable merge in-memory-hashtable: {len(Server.hashtable)} entries")
                
                         

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    server = Server()
    server.start()