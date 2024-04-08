import socket

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


publish("topic1")
publish("topic2")
publish("topic3")


for i in range(10):
    publish("topic4")

for i in range(50):
    publish("topic5")


for i in range(100):
    publish("topic6")

for i in range(500):
    publish("topic9")