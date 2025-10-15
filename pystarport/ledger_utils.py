#!/usr/bin/env python3

import socket

ZEMU_GRPC_SERVER_PORT = 3002
ZEMU_API_PORT = 5001
ZEMU_BUTTON_PORT = 1235


class LedgerButton:
    def __init__(self, host="127.0.0.1", port=ZEMU_BUTTON_PORT):
        self.host = host
        self.port = port
        self._client = None
        self.connected = False

    def connect(self):
        if self.connected:
            return True
        try:
            self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._client.connect((self.host, self.port))
            self.connected = True
            print(f"Connected to button TCP on {self.host}:{self.port}", flush=True)
            return True
        except Exception as e:
            print(f"Failed to connect to button TCP: {e}", flush=True)
            return False

    def _send(self, data):
        if not self.connected and not self.connect():
            return False
        try:
            self._client.send(data.encode())
            return True
        except Exception as e:
            print(f"Button press failed: {e}", flush=True)
            return False

    def press_left(self):
        return self._send("Ll") and print("Pressed left button", flush=True)

    def press_right(self):
        return self._send("Rr") and print("Pressed right button", flush=True)

    def press_both(self):
        return self._send("LRlr") and print("Pressed both buttons", flush=True)

    def disconnect(self):
        if self.connected and self._client:
            try:
                self._client.close()
                self.connected = False
                print("Disconnected from button TCP", flush=True)
            except Exception as e:
                print(f"Failed to disconnect: {e}", flush=True)
