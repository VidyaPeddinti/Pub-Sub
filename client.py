import socket

def get_user_details():
    while True:
        name = input("Enter your name: ")
        if name.strip() and name.isalpha(): 
            return name
        print("Please enter your name, with only alphabets")

host = 'localhost'
port = 5000

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((host, port))

name = get_user_details()

while True:

    print("\n1. Add Money")
    print("2. Withdraw Money")
    print("3. List Transactions")
    print("4. Check Balance")
    print("5. Exit")

    choice = input("Enter your choice (1-5): ")

    if choice == "1" or choice == "2":
        amount = input("Enter the amount: ")
        request_data = f"{name};{choice};{amount}"
    elif choice == "3" or choice == "4":
        request_data = f"{name};{choice}"
    elif choice == "5":
        break
    else:
        print("invalid choice")
        continue
    
    print(request_data)
    client_socket.send(request_data.encode())


    response = client_socket.recv(1024).decode()
    print("recieved:", response)

client_socket.close()
