import sys
from lib.qserver.QServer import *

class Peer(Prototype):
    name = Prototype.String()

class Server(QuickServer):
    class ServerMap(QuickServerMap):
        SEND_HASH_TABLE = 2
        ADD_PEER        = 3
        REMOVE_PEER     = 4
        DELETE_PEER     = 5
        UPDATE_PEER     = 6
        GET_PEER        = 7
        SEND_IDENTITY   = 8

    @JsonMap()
    @Map[int](ServerMap.ADD_PEER)
    def createPeer(self, peer: Peer) -> None:
        print(peer.name)

if __name__ == "__main__":
    server = Server(interface="127.0.0.1", port=8080)
    server.setOnNewConnection(
        decoder=utf8Decoder,
        loader=jsonLoader,
        valueMap=keyMap("messageType")
    )

    server.start()
    sys.exit(0)