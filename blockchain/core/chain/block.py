from typing import Self
from core.transaction import Transaction
from core.ISerializable import ISerializable

class Block(ISerializable):
    class Header(ISerializable):
        def __init__(self, data: dict[str, object]) -> None:
                self.validators                         :list[str]       = data["validators"]
                self.nextValidators                     :dict[str, dict] = data["nextValidators"]
                self.validatorPublicKey                 :str             = data["validatorPublicKey"]
                self.validatorVerificationSequence      :str             = data["validatorVerificationSequence"]
                self.lastBlockHash                      :str             = data["lastBlockHash"]
                self.blockNumber                        :str             = data["blockNumber"]
                self.blockHash                          :str            = ""
            
        def serialize(self) -> dict:
            return {
                "validators": self.validators,
                "validatorPublicKey": self.validatorPublicKey,
                "validatorVerificationSequence": self.validatorVerificationSequence,
                "lastBlockHash": self.lastBlockHash,
                "blockNumber": self.blockNumber,
                "blockHash": self.blockHash,
                "nextValidators": self.nextValidators
            }
    class Payload(ISerializable):
        def __init__(self, data: dict[str, object]) -> None:
            self.sequence: str                      = data["sequence"]
            self.transactions: list[Transaction]    = data["transactions"]

        def serialize(self) -> dict:
            return {
                "sequence": self.sequence,
                "transactions": [transaction.serialize() if isinstance(transaction, ISerializable) else transaction for transaction in self.transactions]
            }

    def __init__(self, header: Header, payload: Payload) -> None:
        self.__header = header
        self.__payload = payload
        
    def serialize(self) -> dict:
        return {
            "__header": self.__header.serialize(),
            "payload": self.__payload.serialize()
        }
        
    def add_transaction(self, transaction: Transaction) -> None:
        self.__payload.transactions.append(transaction)
    
    def get_transactions(self) -> list[Transaction]:
        return self.__payload.transactions
        
    def get_header(self) -> dict:
        return self.__header.serialize()
    
    def get_payload(self) -> dict:
        return self.__payload.serialize()
        
    def set_block_hash(self, hash: str) -> Self:
        self.__header.blockHash = hash
        
    def get_block_hash(self) -> str:
        return self.__header.blockHash
    
    def get_last_hash(self) -> str:
        return self.__header.lastBlockHash
    
    def set_next_validators(self, validators: list[dict]) -> None:
        self.__header.nextValidators = validators
        
    def set_validators(self, validators: list[dict]) -> None:
        self.__header.validators = validators
        
    def add_next_validators(self, id: str, ip: str, port: int):
        self.__header.nextValidators[id] = {
            "ip": ip,
            "port": port
        }
        
        