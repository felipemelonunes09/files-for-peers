from enum import Enum
import sys
import rsa
import hashlib
from QServer import *

class Peer(Prototype):
    name        = Prototype.String(minSize=3, maxSize=100)
    ip          = Prototype.String(pattern="")
    createdAt   = Prototype.DateTime()
    updatedAt   = Prototype.DateTime()
    meta        = Prototype.Dict(
        peerPublicKey = Prototype.String(),
        peerHash      = Prototype.String()
    )
    ports        = Prototype.Dict(
        udhtSync = Prototype.String(patten=""),
        fdhtSync = Prototype.String(patten=""),
        service  = Prototype.String(pattern="")
    )

class Server(QuickServer):
    IDENTITY_DIRECTORY = "data/identity/"
    class ServerMap(QuickServerMap):
        SEND_HASH_TABLE = 2
        ADD_PEER        = 3
        REMOVE_PEER     = 4
        DELETE_PEER     = 5
        UPDATE_PEER     = 6
        GET_PEER        = 7
        SEND_IDENTITY   = 8
        CREATE_IDENTITY = 9

    class ServerCode(Enum):
        CREATED_WITH_SUCCESS = 1

    RSA_KEYS_SIZE: int = 512
    SERVER_ENCODING = "utf-8"

    @Map[int](ServerMap.CREATE_IDENTITY)
    @PrototypeMap()
    def registerIdentity(self, clientConnection: ClientConnection, peer: Peer, keysDir: Prototype.String):
        print("=================================== here =====================================")
        ## validation

        ## logic

        publicKey, privateKey = rsa.newkeys(Server.RSA_KEYS_SIZE)
        peerHash = hashlib.sha256()
        peerHash.update(str(peer).encode(Server.SERVER_ENCODING))
        peerHash = peerHash.hexdigest()

        with open(f"./data/identity/public_key.pem", "wb") as pubFile:
            pubFile.write(publicKey.save_pkcs1())
        
        with open(f"data/identity/private_key.pem", "wb") as privFile:
            privFile.write(privateKey.save_pkcs1())

        clientConnection.sendPackage(JsonPackage(
            payload={},
            msg="Created with success on directory",
            statusCode=Server.ServerCode.CREATED_WITH_SUCCESS
        ))

    @Map[int](ServerMap.ADD_PEER)
    @PrototypeMap()
    def createPeer(self, peer: Peer) -> None:
        print(f"Created peer: {peer.name} with success")

    @Map[int](ServerMap.REMOVE_PEER)
    @PrototypeMap()
    def removedPeer(self, uuid: Prototype.String):
        print(f"Removed peer {uuid} with success")

    def start(self):
        return super().start()

if __name__ == "__main__":
    server = Server(interface="127.0.0.1", port=8000, keepAlive=True, mappedFunctionHandler=ConsumerFunctionHandler(consumersQuantity=5))
    server.setOnNewConnection(decoder=utf8Decoder, loader=jsonLoader, valueMap=keyMap("messageType"))
    server.setMessagePolicy(HeaderMessagePolicy())
    server.start()
    sys.exit(0)
