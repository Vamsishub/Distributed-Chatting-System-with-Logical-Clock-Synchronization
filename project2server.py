import threading
import socket
import time
import queue

PORT_NUMBER = 8642

class To_Handle_Client(threading.Thread):
    def __init__(self, client_soc, client_n0, queue_for_msgs):
        threading.Thread.__init__(self)
        self.client_soc = client_soc
        self.client_n0 = client_n0
        self.logical_clock = 0
        self.queue_for_msgs = queue_for_msgs

    def increment_logical_clock(self):
        self.logical_clock = int(time.time() * 1000)

    def updating_logical_clock(self, rcv_timestamp):
        self.logical_clock = max(self.logical_clock, rcv_timestamp) + 1

    def run(self):
        size_of_the_buffer = 1024
        while True:
            try:
                # Read message from the client
                buffer_value = self.client_soc.recv(size_of_the_buffer).decode()
                if not buffer_value:
                    # If buffer is empty, the client has disconnected
                    print(f"Client #{self.client_n0} disconnected.")
                    break

                rcv_timestamp, message = buffer_value.split(':', 1)
                rcv_timestamp = int(rcv_timestamp)
                self.updating_logical_clock(rcv_timestamp)
                current_time = time.strftime('%H:%M:%S', time.localtime())

                print(f"[{current_time}] Received beautiful message from Client #{self.client_n0}: {message.strip()} (Logical Clock: {self.logical_clock})")
                self.queue_for_msgs.put((self.logical_clock, message.strip())) # here i am adding msg to the queue with its logical clock value for ordering

                #this will Send response back to the client
                server_response = str(self.logical_clock)
                self.increment_logical_clock()
                self.client_soc.send(server_response.encode())

            except ConnectionResetError:
                print(f"Client #{self.client_n0} forcibly disconnected.")
                break

       
        self.client_soc.close() # We can close the client connection using client.close function

def server_broadcast_msgs(queue_for_msgs, client_handlers):
    while True:
        try:
            # we can Get the next message from the queue
            logical_clock, message = queue_for_msgs.get()

            # so this server will Broadcast the message to all clients
            for client_handler in client_handlers:
                client_handler.client_soc.send(f"{logical_clock}:{message}".encode())

        except Exception as e:
            print("Something has went wrong, Error while broadcasting message:", e)

def main():
    server_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_soc.bind(('localhost', PORT_NUMBER))
    server_soc.listen(3)

    print("The server is now online and actively waiting for clients to connect.")

    no_of_clients = 0
    queue_for_msgs = queue.PriorityQueue()
    client_handlers = []

    broadcast_thread = threading.Thread(target=server_broadcast_msgs, args=(queue_for_msgs, client_handlers))
    broadcast_thread.start()

    while True:
        client_soc, address = server_soc.accept()
        no_of_clients += 1
        current_time = time.strftime('%H:%M:%S', time.localtime())
        print(f"[{current_time}]A successful connection to the server has been made from Client #{no_of_clients}! We can chat further...!")

        # Start a new thread to handle the client
        client_handler = To_Handle_Client(client_soc, no_of_clients, queue_for_msgs)
        client_handlers.append(client_handler)
        client_handler.start()

if __name__ == "__main__":
    main()


