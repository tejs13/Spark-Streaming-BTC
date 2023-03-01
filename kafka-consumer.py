from kafka import KafkaConsumer

import socket


# consumer = KafkaConsumer('bitcoin', group_id='active-group')
#
#
# for msg in consumer:
#     print(msg)
client_socket = socket.socket()  # instantiate
client_socket.connect(('localhost', 6666))

while True:
    msg = client_socket.recv(2048).decode()
    print(msg, "=================")





