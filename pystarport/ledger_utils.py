#!/usr/bin/env python3

import socket
import threading
import time

import requests

ZEMU_GRPC_SERVER_PORT = 3002
ZEMU_API_PORT = 5001
ZEMU_BUTTON_PORT = 1235

STATUS_OK = bytes.fromhex("9000")
STATUS_USER_REJECTED = bytes.fromhex("6985")
STATUS_WRONG_LENGTH = bytes.fromhex("6700")
STATUS_TIMEOUT = bytes.fromhex("6408")
STATUS_GENERAL_ERROR = bytes.fromhex("6F00")


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
            return True
        except Exception:
            return False

    def _send(self, data):
        if not self.connected and not self.connect():
            return False
        try:
            self._client.send(data.encode())
            return True
        except Exception:
            return False

    def press_left(self):
        return self._send("Ll")

    def press_right(self):
        return self._send("Rr")

    def press_both(self):
        return self._send("LRlr")

    def disconnect(self):
        if self.connected and self._client:
            try:
                self._client.close()
                self.connected = False
            except Exception:
                pass


class LedgerAPDU:
    def __init__(self, api_port=ZEMU_API_PORT):
        self.api_port = api_port
        self.api_url = f"http://127.0.0.1:{api_port}/apdu"

    def send_apdu(self, apdu_hex, timeout=30):
        try:
            response = requests.post(
                self.api_url,
                json={"data": apdu_hex},
                timeout=timeout,
            )

            if response.status_code == 200:
                data = response.json().get("data", "")
                return bytes.fromhex(data) if data else STATUS_USER_REJECTED
            else:
                return STATUS_USER_REJECTED

        except Exception:
            return STATUS_USER_REJECTED

    def send_apdu_with_automation(self, apdu_hex, automation_func, timeout=120):
        apdu_result = [None]
        apdu_complete = threading.Event()

        def send_apdu():
            try:
                response = requests.post(
                    self.api_url,
                    json={"data": apdu_hex},
                    timeout=timeout,
                )

                if response.status_code == 200:
                    data = response.json().get("data", "")
                    apdu_result[0] = (
                        bytes.fromhex(data) if data else STATUS_USER_REJECTED
                    )
                else:
                    apdu_result[0] = STATUS_USER_REJECTED

            except Exception:
                apdu_result[0] = STATUS_USER_REJECTED
            finally:
                apdu_complete.set()

        # Start APDU request
        threading.Thread(target=send_apdu, daemon=True).start()

        # Start button automation
        if automation_func:
            threading.Thread(
                target=automation_func, args=(apdu_complete,), daemon=True
            ).start()

        # Wait for completion
        success = apdu_complete.wait(timeout=timeout)

        if success and apdu_result[0]:
            return apdu_result[0]

        return STATUS_USER_REJECTED

    @staticmethod
    def is_success(response_bytes):
        return (
            response_bytes
            and len(response_bytes) >= 2
            and response_bytes[-2:] == STATUS_OK
        )


def ethereum_transaction_automation(btn_client, apdu_complete):
    try:
        time.sleep(4)

        if not btn_client.connect():
            return

        # Navigate through screens
        screen_delays = [3, 2.5, 2.5, 2.5, 2]

        for delay in screen_delays:
            if apdu_complete.is_set():
                break
            btn_client.press_right()
            time.sleep(delay)

        # Approval attempts
        if not apdu_complete.is_set():
            for attempt in range(8):
                if apdu_complete.is_set():
                    break

                if attempt < 3:
                    btn_client.press_both()
                    time.sleep(3)
                elif attempt == 3:
                    btn_client.press_left()
                    time.sleep(1.5)
                    btn_client.press_right()
                    time.sleep(1.5)
                    btn_client.press_both()
                    time.sleep(3)
                elif attempt == 4:
                    btn_client.press_right()
                    time.sleep(1)
                    btn_client.press_both()
                    time.sleep(3)
                else:
                    btn_client.press_both()
                    time.sleep(4)

        btn_client.disconnect()

    except Exception:
        pass


class LedgerScreen:
    """Reads and drives the Speculos screen through its REST API."""

    def __init__(self, api_port=ZEMU_API_PORT):
        self.api_url = f"http://127.0.0.1:{api_port}"

    def text(self):
        rsp = requests.get(f"{self.api_url}/events?currentscreenonly=true", timeout=5)
        return " ".join(event["text"] for event in rsp.json()["events"])

    def press(self, button):
        requests.post(
            f"{self.api_url}/button/{button}",
            json={"action": "press-and-release"},
            timeout=5,
        )
        time.sleep(0.5)

    def press_until(self, button, done, timeout=30):
        deadline = time.time() + timeout
        while not done(self.text()):
            if time.time() > deadline:
                raise TimeoutError(f"stuck on Ledger screen: {self.text()}")
            self.press(button)


def enable_blind_signing(screen):
    """Turn on the Ethereum app setting needed to sign EIP-712 hashes."""
    screen.press_until("right", lambda text: "App settings" in text)
    screen.press("both")
    screen.press_until("right", lambda text: "Blind signing" in text)
    if "Enabled" not in screen.text():
        screen.press("both")
    screen.press_until("right", lambda text: text.strip() == "Back")
    screen.press("both")
    screen.press_until("left", lambda text: "is ready" in text)


def eip712_automation(screen, apdu_complete, timeout=60):
    """Accept the blind signing warning, page through the hashes and sign."""
    deadline = time.time() + timeout
    while not apdu_complete.is_set() and time.time() < deadline:
        text = screen.text()
        if "Blind signing ahead" in text or "Sign message" in text:
            screen.press("both")
        elif "Review typed" in text or "hash" in text:
            screen.press("right")
        else:
            time.sleep(0.5)


def cosmos_address_automation(btn_client, apdu_complete):
    try:
        time.sleep(3)
        if not btn_client.connect():
            return

        btn_client.press_right()
        time.sleep(2)
        btn_client.press_right()
        time.sleep(1)
        btn_client.press_both()
        time.sleep(2)

        if not (apdu_complete.is_set() and btn_client.connected):
            btn_client.press_left()
            time.sleep(1)
            btn_client.press_both()
            time.sleep(1)

            if not apdu_complete.is_set():
                btn_client.press_right()
                time.sleep(1)
                btn_client.press_both()
                time.sleep(1)

        btn_client.disconnect()

    except Exception:
        pass
