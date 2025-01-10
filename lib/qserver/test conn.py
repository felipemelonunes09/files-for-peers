import json
import socket
from QServer import HeaderMessagePolicy

def main():
    host = '127.0.0.1'
    port = 8000
    message__add = {
        "messageType": 3, 
        "name":"Alice",
        "ip": "123.0.3.1",
        "uuid": "54542165456-56456465-84745",
        "createdAt": "2024-12-27 19:50:18",
        "updatedAt": "2024-12-27 19:50:18",
        "ports": {
            "consensusPort": 8000,
            "servicePort": 8000,
            "syncUserPort": 8000,
            "syncFilePort":8000
        }
    }	

    message_remove = {
        "messageType": 4,
        "uuid": "54542165456-56456465-84745"
    }

    # Create a socket object
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    messagePolicy = HeaderMessagePolicy()

    try:
        client_socket.sendall(messagePolicy.buildPackage(json.dumps(message__add).encode()))
        #client_socket.sendall(messagePolicy.buildPackage(json.dumps(message_remove).encode()))
    except Exception as e:
        print(f'An error occurred: {e}')

    finally:
        # Close the socket
        client_socket.close()

if __name__ == '__main__':
    main()