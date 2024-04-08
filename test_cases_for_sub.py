import socket
import threading

host = 'localhost'
port = 5051

def subscribe_to_topic(topic):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mq_socket:
            mq_socket.connect(('localhost', 5555))
            print("Connected to message queue")
            
            mq_socket.send("subscriber".encode('utf-8'))
            print("Sent subscriber request")
            
            mq_socket.send(topic.encode('utf-8'))
            print(f"Sent topic: {topic}")
            
            ack = mq_socket.recv(1024).decode('utf-8')
            if ack == "OK":
                print(f"Successfully subscribed to topic: {topic}")
                
                mq_socket.send(f"{host},{port}".encode('utf-8'))
                print("Sent subscriber's address for callback")
                
                ack2 = mq_socket.recv(1024).decode('utf-8')
                if ack2 == "done":
                    print("Subscription confirmed.")
                else:
                    print("Subscription failed.")
            else:
                print("Subscription failed.")

    except Exception as e:
        print(f"Error subscribing to topic '{topic}': {e}")

def handle_messages():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as subscriber_socket:
            subscriber_socket.bind((host, port))
            subscriber_socket.listen()
            print("Subscriber listening for messages...")
            while True:
                conn, addr = subscriber_socket.accept()
                message = conn.recv(1024).decode('utf-8')
                print("Received message:", message)
                response = "ACK"
                conn.send(response.encode("utf-8"))
                conn.close()
    except Exception as e:
        print(f"Error handling messages: {e}")

def main():
    try:
        topic = "topic_withdraw"
        subscribe_to_topic(topic)

        topic1 = "topic1"
        subscribe_to_topic(topic1)

        for i in range(100):
            topic2 = "topic2"
            subscribe_to_topic(topic2)

        for i in range(100):
            topic2 = topic2
            subscribe_to_topic(topic2)

        
        threading.Thread(target=handle_messages).start()

    except Exception as e:
        print(f"Error in main function: {e}")

if __name__ == "__main__":
    main()

