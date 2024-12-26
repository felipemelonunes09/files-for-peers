from core.Peer import Peer
from injector import inject
from singleton_decorator import singleton
from datetime import datetime
import shelve
import pickle

@singleton
class DHTService:
    @inject
    def __init__(self, filename: str) -> None:
        self.__filename = filename
        
    def create_peer(self, peer: Peer) -> bool:     
        with shelve.open(self.__filename) as hash_table:
            registry = hash_table.get(peer.ip, None)
            if not registry:
                hash_table[peer.peer_id] = peer.serialize()
                print(hash_table[peer.peer_id])
                print(peer.peer_id)
                return True
        return False
    
    def update_peer(self, peer: Peer) -> bool:
        with shelve.open(self.__filename) as hash_table:
            registry = hash_table.get(peer.ip, None)
            if registry:
                peer.updated_at = datetime.now()
                hash_table[peer.peer_id] = peer.serialize()
                return True
        return False
    
    def remove_peer(self, peer: Peer) -> bool:
        with shelve.open(self.__filename) as hash_table:
            registry = hash_table.get(peer.ip, None)
            if registry:
                hash_table[peer.peer_id] = None
                del hash_table[peer.peer_id]
                return True
        return False
    
    def get_peer(self, peer_id: str) -> dict | None:
        with shelve.open(self.__filename) as hash_table:
            return hash_table.get(peer_id, None)
    
    def get_hash_table(self) -> dict:
        hash_table_copy = dict()
        with shelve.open(self.__filename) as hash_table:  
            hash_table_copy = pickle.dumps(dict(hash_table))
        return hash_table_copy