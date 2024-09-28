import socket
import threading
import time
import random
import pickle

sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sckt.bind(('', 0))

def send_heartbeat(ip, port):
    while True:
        sckt.sendto('heartbeat'.encode(), (ip, port))
        time.sleep(3)
        # print(f'heartbeat sent to {ip}:{port}')
        
        

def handle_client(sckt, filename):
    with open(filename, 'rb') as file:
            while True:
                # Read file in chunks
                chunk = file.read(1024)
                if not chunk:
                    break
                sckt.sendall(chunk)

    sckt.close()

def upload(file_name, lstn_ip, lstn_port):
    sckt_up = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sckt_up.bind((lstn_ip, lstn_port))
    sckt_up.listen(100)

    while True:
        client, addr = sckt_up.accept()
        client_handler = threading.Thread(target=handle_client, arg=(client,file_name, ))
        client_handler.start()
        

def download(lstn_ip, lstn_port, file_name, trckr_ip, trckr_port):
    print('start downloading')
    dwn_sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dwn_sckt.bind((lstn_ip, lstn_port))
    data, addr = dwn_sckt.recvfrom(1024)
    data = pickle.loads(data)
    log_id = data.pop()

    slctd = random.choice(data)
    ip, port = slctd[3].split(':')
    port = int(port)
    dwn_sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    dwn_sckt.connect((ip, port))

    err = 1
    try:
        with open('a' + file_name, 'wb') as file:
            while True:
                chunk = dwn_sckt.recv(1024)
                if not chunk:
                    break
                file.write(chunk)
    except:
        err = 0

    dwn_sckt.close()
    dwn_sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dwn_sckt.sendto(f'dwn_st {log_id} {err}'.encode(), (trckr_ip, trckr_port))

while True:
    tokens = input().split()
    if tokens[0] == 'share':
        file_name = tokens[1]
        ip, port = tokens[2].split(':')
        port = int(port)
        msg = f'share {file_name} {tokens[3]}'.encode()
        sckt.sendto(msg, (ip, port))
        thrd = threading.Thread(target=send_heartbeat, args=(ip, port))
        thrd.daemon = True
        thrd.start()


        lstn_ip, lstn_port = tokens[3].split(':')
        lstn_port = int(lstn_port)

        thrd2 = threading.Thread(target=upload, args=(file_name, lstn_ip, lstn_port))
        thrd2.start()


    elif tokens[0] == 'get':
        file_name = tokens[1]
        ip, port = tokens[2].split(':')
        port = int(port)
        msg = f'get {file_name} {tokens[3]}'.encode()
        sckt.sendto(msg, (ip, port))
        lstn_ip, lstn_port = tokens[3].split(':')
        lstn_port = int(lstn_port)

        thrd_dwn = threading.Thread(target=download, args=(lstn_ip, lstn_port, file_name, ip, port))
        thrd_dwn.start()