import sys
from QServer import *

class Peer(Prototype):
    name = Prototype.String()
    ip = Prototype.String()
    uuid = Prototype.String()
    createdAt = Prototype.DateTime()
    updatedAt = Prototype.DateTime()
    ports = Prototype.Dict(
        service     = Prototype.Int(),
        consensus   = Prototype.Int(),
        syncUser    = Prototype.Int(),
        syncFile    = Prototype.Int()
    )

class Server(QuickServer):
    class ServerMap(QuickServerMap):
        SEND_HASH_TABLE = 2
        ADD_PEER        = 3
        REMOVE_PEER     = 4
        DELETE_PEER     = 5
        UPDATE_PEER     = 6
        GET_PEER        = 7
        SEND_IDENTITY   = 8

    @Map[int](ServerMap.ADD_PEER)
    @JsonMap()
    def createPeer(self, peer: Peer) -> None:
        print("Created peer with success")

    @Map[int](ServerMap.REMOVE_PEER)
    @JsonMap()
    def removedPeer(uuid: Prototype.String):
        print("Removed peer with success")

if __name__ == "__main__":
    server = Server(interface="127.0.0.1", port=8000, keepAlive=True, mappedFunctionHandler=ConsumerFunctionHandler(consumersQuantity=5))
    server.setOnNewConnection(decoder=utf8Decoder, loader=jsonLoader, valueMap=keyMap("messageType"))
    server.setMessagePolicy(HeaderMessagePolicy())
    server.start()
    sys.exit(0)
