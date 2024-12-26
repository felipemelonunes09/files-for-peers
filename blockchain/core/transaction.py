from enum import Enum
from core.ISerializable import ISerializable
import datetime
import hashlib
import json
import globals

class Transaction(ISerializable): 
    class TransactionCode(Enum):
        HOLD_STAKE  = 1
        UPLOAD      = 2
        DOWNLOAD    = 3

    def __init__(self, code: TransactionCode, peer_id: str, cost: int = 0, reward: int = 0, fee: int = 0) -> None:
        self.code                           = code
        self.peer_id                        = peer_id
        self.cost                           = cost
        self.reward                         = reward
        self.fee                            = fee
        self.hash: str                      = str()
        self.timestamp: datetime.datetime   = None
        super().__init__()
        
    def generate_timestamp(self) -> None:
        self.timestamp = datetime.datetime.now()
        
    def generate_hash(self) -> None:
        hash = hashlib.sha256(json.dumps(self.serialize()).encode(globals.ENCODING))
        dig = hash.hexdigest()
        self.hash = dig
        
    def confirm(self) -> dict:
        self.generate_timestamp()
        self.generate_hash()
        return self.serialize()

    def serialize(self) -> dict[str, object]:
        return {
            "peerId"            : self.peer_id,
            "transactionHash"   : self.hash,
            "transactionCost"   : self.cost,
            "transactionFee"    : self.fee,
            "transactionReward" : self.reward,
            "transactionCode"   : self.code.value,
        }

class HoldStakeTransaction(Transaction):
    
    def __init__(self, peer_id: str, stake: int, ip: str, port: int, **k) -> None:
        self.stake = stake
        self.ip = ip
        self.port = port
        super().__init__(code=Transaction.TransactionCode.HOLD_STAKE, peer_id=peer_id)
    
    def serialize(self) -> dict[str, object]:
        return {
            **super().serialize(),
            "stake": self.stake,
            "ip": self.ip,
            "port": self.port
        } 

class UploadTransaction(Transaction):
    def __init__(self, peer_id: str, filename: str, **k) -> None:
        self.filename = filename
        super().__init__(Transaction.TransactionCode.UPLOAD, peer_id)
    
    def serialize(self) -> dict[str, object]:
        return {
            **super().serialize(),
            "filename": self.filename
        }
        
class DownloadTransaction(Transaction):
    def __init__(self, peer_id: str, filename: str, receiver: str, **k) -> None:
        super().__init__(Transaction.TransactionCode.DOWNLOAD, peer_id)
        self.filename = filename
        self.receiver = receiver
        
    def serialize(self) -> dict[str, object]:
        return {
            **super().serialize(),
            "file_name": self.filename,
            "receiver": self.receiver
        }

def create_transaction(code: int, payload: dict) -> HoldStakeTransaction | UploadTransaction | DownloadTransaction:
    if code == Transaction.TransactionCode.HOLD_STAKE.value:
        return HoldStakeTransaction(**payload)
    elif code == Transaction.TransactionCode.UPLOAD.value:
        return UploadTransaction(**payload)
    elif code == Transaction.TransactionCode.DOWNLOAD.value:
        return DownloadTransaction(**payload)