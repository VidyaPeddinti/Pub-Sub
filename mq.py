import socket
import threading
import time

host = '127.0.0.1'
port = 5555
subscribers = {}

def handle_publisher(conn, addr):
    try:
        while True:
            message = conn.recv(1024).decode('utf-8')
            if not message:
                break
            print(f"Received message from publisher: {message}")
            forward_to_subscribers(message)
    except Exception as e:
        print(f"Error handling publisher connection from {addr}: {e}")
    finally:
        conn.close()

def handle_subscriber(conn, addr):
    global subscribers
    try:
        topic = conn.recv(1024).decode('utf-8')
        print("Received subscription request for topic:", topic)

        if topic in subscribers:
            response = "OK"
        else:
            subscribers[topic] = []
            response = "OK"

        conn.send(response.encode('utf-8'))

        subscriber_addr = conn.recv(1024).decode('utf-8')
        print("Received subscriber's address:", subscriber_addr)

        subscribers[topic].append(subscriber_addr)

        conn.send("done".encode('utf-8'))

    except Exception as e:
        print(f"Error handling subscriber connection from {addr}: {e}")
    finally:
        conn.close()

def forward_to_subscribers(message):
    global subscribers
    try:
        if ":" in message:
            topic, content = message.split(":", 1)
            if topic in subscribers:
                for subscriber_addr in subscribers[topic]:
                    subscriber_host, subscriber_port = subscriber_addr.split(",")
                    try:
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                            s.connect((subscriber_host, int(subscriber_port)))
                            s.send(message.encode('utf-8'))
                            ack = s.recv(1024).decode('utf-8')
                            if ack == "ACK":
                                print(f"Acknowledgment received from {subscriber_host}:{subscriber_port} for topic '{topic}'")
                            else:
                                print(f"Invalid acknowledgment received from {subscriber_host}:{subscriber_port} for topic '{topic}'")
                    except Exception as e:
                        print(f"Error occurred while sending message to {subscriber_host}:{subscriber_port}: {e}")
            else:
                print(f"No subscribers found for topic: {topic}")
    except Exception as e:
        print(f"Error in message forwarding: {e}")

def main():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mq_socket:
            mq_socket.bind((host, port))
            mq_socket.listen()
            print("MQ started, waiting for connections...")
            while True:
                conn, addr = mq_socket.accept()
                try:
                    request = conn.recv(1024).decode('utf-8')
                    if request.startswith("pub"):
                        threading.Thread(target=handle_publisher, args=(conn, addr)).start()
                    elif request.startswith("subscriber"):
                        print("Subscriber connected")
                        threading.Thread(target=handle_subscriber, args=(conn, addr)).start()
                    else:
                        print("Unknown connection")
                        conn.close()
                except Exception as e:
                    print(f"Error handling connection from {addr}: {e}")
                    conn.close()
    except Exception as e:
        print(f"Error in main function: {e}")

if __name__ == "__main__":
    main()
