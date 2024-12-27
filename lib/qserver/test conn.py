import json
import socket

def main():
    host = '127.0.0.1'
    port = 8080
    message = {"messageType":3}

    # Create a socket object
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # Connect to the server
        client_socket.connect((host, port))

        # Send the message
        client_socket.sendall(json.dumps(message).encode())

        # Receive the response
        response = client_socket.recv(1024)
        print('Received from server:', response.decode())

    except Exception as e:
        print(f'An error occurred: {e}')

    finally:
        # Close the socket
        client_socket.close()

if __name__ == '__main__':
    main()