import socket
import json
from typing import Dict

SOCKET_SEND_TIMEOUT = 0.01

def send_alert_to_socket(socket_key: str, payload: Dict):
    """
    socket_key is expected as "host:port" or "host,port" or JSON containing host/port.
    We will attempt parsing safely.
    """
    # try parse host:port
    if not socket_key:
        print("Empty socket key; skipping alert send.")
        return False

    host = None
    port = None
    try:
        if ":" in socket_key:
            host, port_s = socket_key.split(":", 1)
            port = int(port_s)
        elif "," in socket_key:
            host, port_s = socket_key.split(",", 1)
            port = int(port_s)
        else:
            # maybe just a host (no port); default port?
            host = socket_key.strip()
            port = 80
    except Exception as ex:
        print(f"Failed parsing socket key `{socket_key}`: {ex}")
        return False

    try:
        with socket.create_connection((host, port), timeout=SOCKET_SEND_TIMEOUT) as s:
            s.settimeout(SOCKET_SEND_TIMEOUT)
            data = json.dumps(payload).encode("utf-8")
            s.sendall(data)
            print(f"Alert sent to {host}:{port}: {payload}")
        return True
    except Exception as ex:
        print(f"Failed to send alert to {host}:{port}: {ex}")
        return False
