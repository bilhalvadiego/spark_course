import socket
import time

HOST = 'localhost'
PORT = 3000

s = socket.socket()
s.connect((HOST, PORT))

controle = True
while controle:
    data = s.recv(1024)
    print(data.decode("utf-8"))
    time.sleep(2)
    controle = False if data.decode("utf-8") == 'fim' else True