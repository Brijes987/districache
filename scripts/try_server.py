"""Tiny RESP client for trying the DistriCache server by hand.  Usage: python scripts/try_server.py [port]"""
import socket
import sys
import time

port = int(sys.argv[1]) if len(sys.argv) > 1 else 6380
sock = socket.create_connection(("127.0.0.1", port), timeout=3)


def send(*args):
    payload = b"*%d\r\n" % len(args)
    for a in args:
        data = a.encode()
        payload += b"$%d\r\n%s\r\n" % (len(data), data)
    sock.sendall(payload)
    time.sleep(0.05)
    return sock.recv(65536).decode(errors="replace")


for command in [
    ("PING",),
    ("SET", "user:1", "Asha"),
    ("GET", "user:1"),
    ("GET", "missing"),
    ("SET", "session", "token", "EX", "30"),
    ("DBSIZE",),
    ("DEL", "user:1", "missing"),
    ("DBSIZE",),
    ("BOGUS",),
]:
    print(" ".join(command).ljust(32), "->", repr(send(*command)))
sock.close()
