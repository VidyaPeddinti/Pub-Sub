import socket

def withdraw_money(balance, transactions, amount):
    if amount > 0:
        balance -= amount
        transactions.append(f"Debit: {amount}")
        return balance, transactions,f"balance:  {balance}"
    else:
        return balance, transactions,"Please enter an amount greater than zero"

host = 'localhost'
port = 8001

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((host, port))
server_socket.listen()

print(f"Withdrawal Server listening on {host}:{port}")

user_details = {}

while True:
    client_socket, addr = server_socket.accept()
    print(f"Connection from {addr}")

    
    balance_withdrawal = 0
    transactions_withdrawal = []



    while True:
        data = client_socket.recv(1024).decode()

        if not data:
            break

        message = data

        data = data.split(";")
        name = data[0]
        print(name)
        choice = data[1]
        print(choice)


        if name not in user_details:
            user_details[name] = {"balance": balance_withdrawal, "transactions": transactions_withdrawal}

        balance_withdrawal = user_details[name]["balance"]
        transactions_withdrawal = user_details[name]["transactions"]


        if choice == "get_trans":
            response = f"trans: {transactions_withdrawal}"
        elif choice == "get_balance":
            response = f"balance: {balance_withdrawal}" 

        elif choice == "2":
            params = data[2]
       
            amount = float(params)

            balance_withdrawal, transactions_withdrawal, response = withdraw_money(balance_withdrawal, transactions_withdrawal, amount)
        else:
            response = "invalid choice"

        user_details[name]["balance"] = balance_withdrawal
        user_details[name]["transactions"] = transactions_withdrawal
       
            
        client_socket.send(response.encode())

    client_socket.close()
