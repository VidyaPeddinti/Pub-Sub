import socket
import threading

def send_request_to_server_api(host, port, message):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    client_socket.connect((host, port))
    print("connected to server api")

    request = f"{message}"

    client_socket.send(request.encode())

    response = client_socket.recv(1024).decode()
    print("received response from server api")

    client_socket.close()
    
    return response


host = 'localhost'
port = 5000

api_ports = {
    "add_balance": 7000,
    "withdrawal": 8000,
    "passbook": 9000
}

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host, port))
server_socket.listen()

print(f"Client API is listening on {host}:{port}")


def handle_client(client_socket):
    while True:
        data = client_socket.recv(1024).decode()

        if not data:
            break

        message = data

        name, choice, *params = data.split(";")

        response = ""  

        if choice == "1":
            response = send_request_to_server_api('localhost', api_ports["add_balance"], message)
            print("sending req to add api")
        elif choice == "2":
            response = send_request_to_server_api('localhost', api_ports["withdrawal"], message)
            print("sending req to withdraw api")
        elif choice == "3" or choice == "4":
            response = send_request_to_server_api('localhost', api_ports["passbook"], message)
            print("sending req to pass api")
        
        client_socket.send(response.encode())

    client_socket.close()
    print("connection closed")

    
while True:
    client_socket, addr = server_socket.accept()
    client_thread = threading.Thread(target=handle_client, args=(client_socket,))
    client_thread.start()
