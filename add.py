import socket

def add_money(balance, transactions, amount):
    if amount > 0:
        balance += amount
        transactions.append(f"Credit: {amount}")

        return  balance, transactions, f"balance: {balance}" 
    else:
        return balance, transactions, "Please enter an amount greater than zero"

user_details = {}

host = 'localhost'
port_add_balance = 7001

server_socket_add_balance = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket_add_balance.bind((host, port_add_balance))
server_socket_add_balance.listen()


print(f"Add Balance Server listening on {host}:{port_add_balance}")

while True:
    client_socket, addr = server_socket_add_balance.accept()
    print(f"Connection from {addr}")

    balance_add_balance = 0
    transactions_add_balance = []


    while True:
        data = client_socket.recv(1024).decode()

        if not data:
            break

        message = data

        data = data.split(";")
        name = data[0]
        choice = data[1]

        if name not in user_details:
            user_details[name] = {"balance": balance_add_balance, "transactions": transactions_add_balance}

        balance_add_balance = user_details[name]["balance"]
        transactions_add_balance = user_details[name]["transactions"]
        
        if choice == "1":
            params =  data[2]
            amount = float(params)
            balance_add_balance, transactions_add_balance, response = add_money(balance_add_balance, transactions_add_balance, amount)
        elif choice == "get_balance":
            response = f"balance: {balance_add_balance}"     ##sends the balance in the only add service. which means even when the user withdraw money from the withdraw server, it does not affect here. 
        elif choice == "get_trans":
            response = f"trans: {transactions_add_balance}"
        else:
            response = "invalid choice"

        

        user_details[name]["balance"] = balance_add_balance
        user_details[name]["transactions"] = transactions_add_balance
        
        client_socket.send(response.encode())

        

    client_socket.close()