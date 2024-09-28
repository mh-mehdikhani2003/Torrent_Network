import sqlite3
import socket
import json
import threading
import time
import pickle
from tabulate import tabulate
import os

try:
    os.remove('tracker.db')
except:
    pass

conn = sqlite3.connect('tracker.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, addr TEXT, last_heartbeat DATETIME)')
cursor.execute('CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT, file_name TEXT, addr TEXT, peer TEXT)')
conn.commit()

cursor.execute('CREATE TABLE IF NOT EXISTS request_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, file_name TEXT, peer INTEGER, seeders TEXT, success INTEGER)')
conn.commit()


# IP = input('Enter the IP: ')
# PORT = int(input('Enter the Port: '))

IP = '127.0.0.1'
PORT = 6771

sckt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sckt.bind((IP, PORT))


def check_heartbeat():
    while True:
        if not conn.in_transaction:
            cursor.execute('''DELETE FROM files WHERE addr IN (SELECT addr FROM users WHERE 
                                    (julianday("now") - julianday(last_heartbeat)) * 86400 > 5)''')
            
            cursor.execute('DELETE FROM users WHERE (julianday("now") - julianday(last_heartbeat)) * 86400 > 5')
            conn.commit()
            time.sleep(4)
        

def server():
    cursor = conn.cursor()
    while True:
        data, addr = sckt.recvfrom(1024)
        req = data.decode()
        tokens = req.split()

        cursor.execute('SELECT * FROM users WHERE addr = ?', (f'{addr[0]}:{addr[1]}',))
        res = cursor.fetchall()
        if len(res) == 0:
            print(f'Adding user {addr}')
            cursor.execute('INSERT INTO users (addr, last_heartbeat) VALUES (?, datetime("now"))', (f'{addr[0]}:{addr[1]}',))
            conn.commit()

        if tokens[0] == 'share':
            # share filename ip:port
            file_name = tokens[1]
            peer = tokens[2]
            cursor.execute('INSERT INTO files (file_name, addr, peer) VALUES (?, ?, ?)', (file_name, f'{addr[0]}:{addr[1]}', peer))
            cursor.execute('SELECT id FROM users WHERE addr = ?', (f'{addr[0]}:{addr[1]}',))
            id = cursor.fetchone()[0]
            cursor.execute('INSERT INTO request_logs (type, file_name, peer) VALUES (?, ?, ?)', (0, file_name, id,))
            conn.commit()
        elif tokens[0] == 'get':
            print('get')
            file_name = tokens[1]
            cursor.execute('SELECT * FROM files WHERE file_name = ?', (file_name,))
            rows = cursor.fetchall()
            cursor.execute('SELECT id FROM users WHERE addr = ?', (f'{addr[0]}:{addr[1]}',))
            id = cursor.fetchone()[0]
            ids = cursor.execute('SELECT id FROM users WHERE addr IN (SELECT addr FROM files WHERE file_name = ?)', (file_name,))
            cursor.execute('INSERT INTO request_logs (type, file_name, peer, seeders) VALUES (?, ?, ?, ?)', (1, file_name, id, ','.join([str(id[0]) for id in ids])))
            log_id = cursor.lastrowid
            rows.append(log_id)
            sckt.sendto(pickle.dumps(rows), (tokens[2].split(':')[0], int(tokens[2].split(':')[1])))
            conn.commit()
        elif tokens[0] == 'heartbeat':
            # print(f'heartbeat received, {(f'{addr[0]}:{addr[1]}')}')
            cursor.execute('UPDATE users SET last_heartbeat = datetime("now") WHERE addr=?', (f'{addr[0]}:{addr[1]}', ))
            conn.commit()
        elif tokens[0] == 'dwn_st':
            log_id = int(tokens[1])
            st = int(tokens[2])
            cursor.execute('UPDATE request_logs SET success = ? WHERE id = ?', (st, log_id))
            conn.commit()

srvr = threading.Thread(target=server)
srvr.start()
hrt = threading.Thread(target=check_heartbeat)
hrt.start()

while True:
    command = input()
    if command == 'request logs':
        #باید پیر هائی که درخواست دادن بگیرن فایلو ولی نتونستن هم باشن
        cursor.execute('SELECT peer, file_name, seeders, success FROM request_logs WHERE type = 1')
        rows = cursor.fetchall()
        headers = ['peer', 'file', 'seaders', 'success']
        print(tabulate(rows, headers=headers))

    elif command == 'all-logs':
        #همه پییر هائی که فایل مربوطه را دارند (طبیعتا موفق شده ها)
        cursor.execute('SELECT peer, file_name FROM request_logs WHERE type = 0')
        rows = cursor.fetchall()
        headers = ['peer', 'file']
        print(tabulate(rows, headers=headers))
    
    elif command.startswith('file_logs'):
        file_name = command.split()[1]
        #پیام خطا نداره و اینکه کل ستون های مربوطه با فقط ستونی که فایل مربوطه بود چاپ شود
        cursor.execute('SELECT type, peer, seeders, success FROM request_logs WHERE file_name = ?', (file_name,))
        rows = cursor.fetchall()
        headers = ['type', 'peer', 'seeders', 'success']
        print(tabulate(rows, headers=headers))
        print('* types are 0=upload, 1=download')