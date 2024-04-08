##publisher

import socket
import threading


host = 'localhost'
port = 8000
withdraw_port = 8001
pass_api_port = 9000 ##subscriber
mq_port = 5555
mq_host = '127.0.0.1'


def publish(message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as pub_socket:
            pub_socket.connect((mq_host, mq_port))  
            pub_socket.send("pub".encode('utf-8'))
            print(f"Sending: {message}")
            pub_socket.send(message.encode('utf-8'))
        pub_socket.close()
        
    except Exception as e:
        print(f"Error publishing message to MQ: {e}")
        

def send_request_to_server(host, port, message):
    client1_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    client1_socket.connect((host, port))
    print("connected to withdraw server")

    request = f"{message}"

    client1_socket.send(request.encode())

    response = client1_socket.recv(1024).decode()
    print("received from withdraw server")
    publish(f"topic_withdraw: {response}")

    client1_socket.close()

    return response

def handle_client(client_socket, withdraw_host, withdraw_port):

    print(f"Connection from {addr}")
    
    while True:
        data = client_socket.recv(1024).decode()
        print("received data from client api", data)
        if not data:
            break
        response = send_request_to_server(withdraw_host, withdraw_port, data)
        print("sending response to cleint_api:", response)
        client_socket.send(response.encode())
    client_socket.close()


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host, port))
server_socket.listen()

print(f"Withdraw Server api listening on {host}:{port}")

while True:
    client_socket, addr = server_socket.accept()
    client_thread = threading.Thread(target=handle_client, args=(client_socket, host, withdraw_port))
    client_thread.start()
