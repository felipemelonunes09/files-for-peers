from injector import Module, provider
from core.Peer import Peer
from core.DHTService import DHTService
import globals
import shelve

class DHTModule(Module):
    
    @provider
    def provide_dht_service(self) -> DHTService:
        return DHTService(globals.D_HASH_TABLE_NAME)