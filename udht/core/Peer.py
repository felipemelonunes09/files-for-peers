from __future__ import annotations
from datetime import datetime
from io import TextIOWrapper
import json
from socket import socket
from typing import Dict, Union

class Peer:

    ptuple = tuple[int, dict, tuple, socket]
    def __init__(self, name: str, ip: str, ports: dict, peer_id: str) -> None:
        self.peer_id = peer_id
        self.name = name
        self.ip = ip
        self.ports = ports
        self.created_at = str(datetime.now())
        self.updated_at = str(datetime.now())
        self.last_connection_on = str(datetime.now())
        
    def serialize(self) -> Dict[str, Union[str, str]]:
        return {
            "peer_id": self.peer_id,
            "name": self.name,
            "ip": self.ip,
            "ports": self.ports,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "lastConnectionOn": self.last_connection_on
        }
        
    
    @staticmethod
    def load_identity(file: TextIOWrapper) -> Peer:
        data = file.read()
        json_obj: dict = json.loads(data)
        return Peer(
            peer_id     = json_obj["peer_id"],
            name        = json_obj["name"],
            ip          = json_obj["ip"],
            ports       = json_obj["ports"]
        )
        
        
