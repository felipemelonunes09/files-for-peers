import os
from injector import Injector
from core.DHTModule import DHTModule

# Global Constants
D_HASH_TABLE_NAME = "data/duser-hash-table"

# Dependency Injection
injector = Injector([DHTModule()])

# Configuration Constants
CONSUMERS_QUANTITY = 4

HOST = "127.0.0.1"
PORT = int(os.getenv("DHT_PORT", 3002))  # Ensure PORT is an integer

# Encoding Settings
BASIC_DECODER = 'utf-8'

# Socket Settings
SOCKETS_CONNECTION_LIMIT = 10

IDENTITY_FILE = "data/identity-file.json"