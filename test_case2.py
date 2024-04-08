##change the names in the test_Case list every time you run this file

import socket


def is_valid_number(input_str):
    return input_str.isalpha()


def is_valid_name(input_str):
    return input_str is not None and input_str.isalpha()

def get_user_details():
    while True:
        name = input("Enter your name: ")
        if name.strip():  
            return name
        print("Name cannot be empty. Please enter your name.")

host = 'localhost'
port = 5000

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((host, port))

#name = get_user_details()

count = 0

while count < 100:

    test_cases = [
    ("Us1", "1", "100", "balance: 100.0"),  
    ("Us1", "2", "50", "'balance: -50.0'"),   
    ("Us1", "3", "", "Final transaction: 'Credit: 100.0', 'Debit: 50.0'"),  
    ("Us1", "4","", "Final Balance: 50.0"),  
    ("Us2", "4", "", "Final Balance: 0.0"),       
    ("Invalid", "6"," ", "invalid choice"),
    ("vid", "5", " ", " ")]
    ##    ("Vid", None, "50", "invalid choice or name or amount"),  ##this will not work but ask the user to enter name until they enter
##    (None, "3", "", "invalid choice"),           ##this will not work but ask the user to enter name until they enter
 ##       ("Vid", "1", "hi", "invalid choice or name or amount"), this will be taken care in the client code itself
 ##       ("4", "2", "50", "invalid choice or name or amount"), this will be taken care in the client code itself


    for name, choice, amount, expected_output in test_cases:
      
        if choice == "5":
            break

        elif choice in {"3", "4"}:
           request_data = f"{name};{choice}"

        elif choice in {"1", "2"}:
           request_data = f"{name};{choice};{amount}"
        
        else:
            print("invalid choice or name or amount")
            continue


        client_socket.send(request_data.encode())
        actual_output = client_socket.recv(1024).decode()

        try:
           assert actual_output == expected_output
        except AssertionError:
           print(f"For input {name}, {choice}, {amount}, expected {expected_output} but got {actual_output}")

    count += 1
    print(count)


client_socket.close()
