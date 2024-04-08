import socket

def show_final_balance(x, y):

    b1 = x.split("balance: ")
    b2 = y.split("balance: ")

    b3 = float(b1[1])
    b4 = float(b2[1])
   
    balance = b3 + b4

    return f"Final Balance: {balance}"

def show_final_trans(x, y):
    if not x:
        t1 = "No credits yet"
    if not y:
        t2 = "No debits yet"

    t1 = x.split("trans: ")[1].strip("[]")

    t2 = y.split("trans: ")[1].strip("[]")

    transactions = f"{t1}, {t2}"  

    return f"Final transaction: {transactions}"

host = 'localhost'
port_passbook = 9001

server_socket_add_balance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket_add_balance.bind((host, port_passbook))
server_socket_add_balance.listen()

print(f"Passbook Server listening on {host}:{port_passbook}")


while True:
    client_socket, addr = server_socket_add_balance.accept()

    print(f"Connection from {addr}")

    while True:
        data = client_socket.recv(1024).decode()
        
        if not data:
            break

        message = data

        choice, x, y = data.split(";")

        if choice == "3":
            response =  show_final_trans(x, y)
         
        elif choice == "4":
            response =  show_final_balance(x, y)
         
        else:
            response = "invalid choice"
       
        client_socket.send(response.encode())

    client_socket.close()


