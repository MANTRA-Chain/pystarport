#!/usr/bin/env python3

import os
import signal
import subprocess
import sys
import threading
import time
from concurrent import futures

import requests


def ensure_grpcio_installed():
    try:
        import grpc

        print("grpcio is already available", flush=True)
        return True
    except ImportError:
        print("grpcio not found, installing...", flush=True)
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "grpcio",
                    "grpcio-tools",
                    "--quiet",
                ],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode == 0:
                print("grpcio installed successfully", flush=True)
                import grpc  # noqa: F401

                return True
            print(f"Failed to install grpcio: {result.stderr}", flush=True)
        except Exception as e:
            print(f"Error installing grpcio: {e}", flush=True)
        return False


if not ensure_grpcio_installed():
    print("Failed to install grpcio", flush=True)
    sys.exit(1)


import grpc


# Minimal mock protobuf classes for ExchangeRequest/Reply
class ExchangeRequest:
    def __init__(self):
        self.command = b""

    def ParseFromString(self, data):
        if len(data) > 2 and data[0] == 0x0A:
            length = data[1]
            self.command = data[2 : 2 + length] if len(data) >= 2 + length else data[2:]
        else:
            self.command = data
        return len(data)


class ExchangeReply:
    def __init__(self, reply=b""):
        self.reply = reply

    def SerializeToString(self):
        length = len(self.reply)
        if length < 128:
            return bytes([0x0A, length]) + self.reply
        # Multi-byte length encoding
        length_bytes = []
        l = length
        while l > 127:
            length_bytes.append((l & 0x7F) | 0x80)
            l >>= 7
        length_bytes.append(l & 0x7F)
        return bytes([0x0A]) + bytes(length_bytes) + self.reply


def deserialize_exchange_request(data):
    req = ExchangeRequest()
    req.ParseFromString(data)
    return req


def serialize_exchange_reply(reply):
    return reply.SerializeToString()


def add_ZemuCommandServicer_to_server(servicer, server):
    handler = grpc.unary_unary_rpc_method_handler(
        servicer.Exchange,
        request_deserializer=deserialize_exchange_request,
        response_serializer=serialize_exchange_reply,
    )
    service = grpc.method_handlers_generic_handler(
        "ledger_go.ZemuCommand", {"Exchange": handler}
    )
    server.add_generic_rpc_handlers([service])


def start_speculos():
    print("Starting Speculos...", flush=True)
    ledger_binary = os.getenv("LEDGER_BINARY", "/tmp/app.elf")
    if not os.path.exists(ledger_binary):
        print(f"Ledger binary not found at {ledger_binary}", flush=True)
        return None

    speculos_cmd = [
        "/home/zondax/speculos/speculos.py",
        "--model",
        "nanos",
        ledger_binary,
        "--display",
        "headless",
        "--apdu-port",
        os.getenv("ZEMU_APDU_PORT", "9999"),
        "--api-port",
        os.getenv("ZEMU_API_PORT", "5001"),
        "--button-port",
        os.getenv("ZEMU_BUTTON_PORT", "1235"),
    ]

    try:
        proc = subprocess.Popen(
            speculos_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        threading.Thread(
            target=lambda: [
                print(f"Speculos: {line.strip()}", flush=True) for line in proc.stdout
            ],
            daemon=True,
        ).start()
        print(f"Speculos started with PID {proc.pid}", flush=True)
        return proc
    except Exception as e:
        print(f"Failed to start Speculos: {e}", flush=True)
        return None


class LedgerButton:
    def __init__(self, host="127.0.0.1", port=None):
        self.host = host
        self.port = port or int(os.getenv("ZEMU_BUTTON_PORT", "1235"))
        self._client = None
        self.connected = False

    def connect(self):
        if self.connected:
            return True
        try:
            import socket

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


class SpeculosGRPCBridge:
    def __init__(self):
        self.host = "127.0.0.1"
        self.port = int(os.getenv("ZEMU_API_PORT", "5001"))
        self.api_url = f"http://{self.host}:{self.port}/apdu"
        print(f"Speculos API: {self.api_url}", flush=True)
        self._test_speculos_connection()

    def _test_speculos_connection(self):
        for attempt in range(10):
            try:
                print(
                    f"Testing Speculos connection (attempt {attempt+1}/10)...",
                    flush=True,
                )
                resp = requests.get(f"http://{self.host}:{self.port}/", timeout=5)
                if resp.status_code == 200:
                    apdu_resp = requests.post(
                        self.api_url, json={"data": "B001000000"}, timeout=10
                    )
                    if apdu_resp.status_code == 200:
                        print("Speculos APDU test passed", flush=True)
                        return True
                    print(f"APDU test failed: {apdu_resp.status_code}", flush=True)
            except requests.exceptions.ConnectionError:
                print("Speculos not ready yet, waiting 2s...", flush=True)
            except Exception as e:
                print(f"Connection test error: {e}", flush=True)
            time.sleep(2)
        print("Failed to connect to Speculos after all retries", flush=True)
        return False

    def _get_button_client(self):
        return LedgerButton()

    def Exchange(self, request, context):
        try:
            cmd = request.command
            cmd_hex = cmd.hex()
            print(f"gRPC Exchange received: {len(cmd)} bytes", flush=True)
            print(f"   Command: {cmd_hex}", flush=True)

            if len(cmd) >= 2:
                cla, ins = cmd[0], cmd[1]
                print(
                    f"   APDU: CLA={cla:02x} INS={ins:02x}"
                    + (f" P1={cmd[2]:02x} P2={cmd[3]:02x}" if len(cmd) > 3 else ""),
                    flush=True,
                )

                # Handle GET_ADDRESS command
                if ins == 0x04 and len(cmd) >= 4 and cmd[2] == 0x01:
                    print(
                        "   GET_ADDRESS command detected - manual buttons", flush=True
                    )
                    return self._handle_address_request(cmd)

                # Handle SIGN command - transaction signing
                elif ins == 0x02:
                    print("   SIGN command detected - transaction signing", flush=True)
                    return self._handle_transaction_signing(cmd)

            # For all other commands, send directly to Speculos
            resp = requests.post(
                self.api_url,
                json={"data": cmd_hex},
                timeout=30,
                headers={"Content-Type": "application/json"},
            )
            print(f"Speculos response: HTTP {resp.status_code}", flush=True)
            if resp.status_code != 200:
                print(f"HTTP error: {resp.status_code}", flush=True)
                return ExchangeReply(reply=bytes.fromhex("6985"))

            result = resp.json()
            response_hex = result.get("data", "")
            if not response_hex:
                print("No data in Speculos response", flush=True)
                return ExchangeReply(reply=bytes.fromhex("6985"))

            response_bytes = bytes.fromhex(response_hex)
            status = response_bytes[-2:] if len(response_bytes) >= 2 else b""
            print(
                f"gRPC response: {len(response_bytes)} bytes - {response_hex}",
                flush=True,
            )

            status_msgs = {
                b"\x90\x00": "Success (9000)",
                b"\x69\x85": "User rejected (6985)",
                b"\x69\x86": "Command not allowed (6986)",
            }
            print(
                "   " + status_msgs.get(status, f"Status: {status.hex()}"), flush=True
            )

            return ExchangeReply(reply=response_bytes)

        except Exception as e:
            print(f"gRPC Exchange error: {e}", flush=True)
            return ExchangeReply(reply=bytes.fromhex("6985"))

    def _handle_transaction_signing(self, cmd):
        """Handle transaction signing with automated button presses"""
        print("Starting transaction signing automation", flush=True)

        # For transaction signing, we need to handle the final confirmation step
        p1, p2 = cmd[2], cmd[3] if len(cmd) >= 4 else (0, 0)

        # Check if this is the final signing step (P1=02)
        if p1 == 0x02:
            print("Final signing step detected - need user confirmation", flush=True)
            return self._handle_final_transaction_approval(cmd)

        # For other signing steps (P1=00, P1=01), send directly to Speculos
        try:
            resp = requests.post(
                self.api_url,
                json={"data": cmd.hex()},
                timeout=10,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                result = resp.json()
                response_hex = result.get("data", "")
                if response_hex:
                    response_bytes = bytes.fromhex(response_hex)
                    print(f"Signing step response: {response_hex}", flush=True)
                    return ExchangeReply(reply=response_bytes)

            print(f"Signing step failed: HTTP {resp.status_code}", flush=True)
            return ExchangeReply(reply=bytes.fromhex("6985"))

        except Exception as e:
            print(f"Signing step error: {e}", flush=True)
            return ExchangeReply(reply=bytes.fromhex("6985"))

    def _handle_final_transaction_approval(self, cmd):
        print("Handling final transaction approval", flush=True)

        apdu_result = [None]
        apdu_complete = threading.Event()

        def send_apdu():
            try:
                print("Sending final signing APDU...", flush=True)
                response = requests.post(
                    self.api_url,
                    json={"data": cmd.hex()},
                    timeout=60,  # Longer timeout for user interaction
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 200:
                    data = response.json().get("data", "")
                    apdu_result[0] = (
                        bytes.fromhex(data) if data else bytes.fromhex("6985")
                    )
                    print(f"Final signing completed: {data}", flush=True)
                else:
                    apdu_result[0] = bytes.fromhex("6985")
                    print(
                        f"Final signing HTTP error: {response.status_code}", flush=True
                    )
            except requests.exceptions.Timeout:
                apdu_result[0] = bytes.fromhex("6408")
                print("Final signing timed out", flush=True)
            except Exception as e:
                apdu_result[0] = bytes.fromhex("6985")
                print(f"Final signing failed: {e}", flush=True)
            finally:
                apdu_complete.set()

        def transaction_approval():
            try:
                # Wait for transaction to be displayed
                time.sleep(3)
                btn_client = self._get_button_client()
                if not btn_client.connect():
                    print(
                        "Failed to connect to buttons for transaction approval",
                        flush=True,
                    )
                    return

                print("Starting transaction approval sequence...", flush=True)
                # Based on the screen sequence, we need to navigate through:
                # 1. Chain ID screen
                # 2. Account screen
                # 3. Sequence screen
                # 4. Type screen
                # 5. Amount screen
                # 6. From address [1/2]
                # 7. From address [2/2]
                # 8. To address [1/2]
                # 9. To address [2/2]
                # 10. Fee screen
                # 11. Gas screen
                # 12. APPROVE screen (STOP HERE)
                # 13. REJECT screen (DON'T GO HERE)
                screens_to_navigate = 11  # Stop at APPROVE screen (don't go to REJECT)

                for i in range(screens_to_navigate):
                    if apdu_complete.is_set():
                        print("Transaction completed during navigation", flush=True)
                        break

                    print(f"Navigating screen {i+1}/{screens_to_navigate}", flush=True)
                    btn_client.press_right()
                    time.sleep(2)  # Delay for screen transitions

                # Now we should be on the APPROVE screen
                if not apdu_complete.is_set():
                    print(
                        "Should be on APPROVE screen - attempting approval", flush=True
                    )

                    # Try approval multiple times
                    for attempt in range(6):
                        if apdu_complete.is_set():
                            print("Transaction approved successfully!", flush=True)
                            break

                        print(f"Approval attempt {attempt+1}/6", flush=True)
                        btn_client.press_both()
                        time.sleep(2.5)

                        # If we're not approved yet and it's early attempts,
                        # maybe we need to navigate back to APPROVE from REJECT
                        if attempt == 2 and not apdu_complete.is_set():
                            print(
                                "Trying to navigate back to APPROVE screen", flush=True
                            )
                            btn_client.press_left()  # Go back from REJECT to APPROVE
                            time.sleep(1.5)
                        elif attempt == 4 and not apdu_complete.is_set():
                            print("Final attempt - ensure we're on APPROVE", flush=True)
                            btn_client.press_left()  # Go back to APPROVE if on REJECT
                            time.sleep(1)

                    if not apdu_complete.is_set():
                        print("All approval attempts failed", flush=True)

                btn_client.disconnect()
                print("Transaction approval sequence completed", flush=True)

            except Exception as e:
                print(f"Transaction approval failed: {e}", flush=True)

        # Start both threads
        threading.Thread(target=send_apdu, daemon=True).start()
        threading.Thread(target=transaction_approval, daemon=True).start()

        if apdu_complete.wait(timeout=60) and apdu_result[0]:
            print("Transaction signing completed successfully", flush=True)
            return ExchangeReply(reply=apdu_result[0])

        print("Transaction signing failed or timed out", flush=True)
        return ExchangeReply(reply=bytes.fromhex("6985"))

    def _handle_address_request(self, cmd):
        print("GET_ADDRESS detected - starting address verification", flush=True)
        p1 = cmd[2] if len(cmd) >= 3 else None

        # Try silent mode first (P1=00)
        silent_cmd = bytearray(cmd)
        silent_cmd[2] = 0x00

        try:
            resp = requests.post(
                self.api_url,
                json={"data": silent_cmd.hex()},
                timeout=10,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code == 200:
                res = resp.json()
                hex_data = res.get("data", "")
                if hex_data and bytes.fromhex(hex_data)[-2:] == b"\x90\x00":
                    print("Silent mode succeeded, returning result", flush=True)
                    return ExchangeReply(reply=bytes.fromhex(hex_data))
                print("Silent mode failed", flush=True)
        except Exception as e:
            print(f"Silent mode test failed: {e}", flush=True)

        # Interactive mode fallback
        apdu_result = [None]
        apdu_complete = threading.Event()

        def send_apdu():
            try:
                response = requests.post(
                    self.api_url,
                    json={"data": cmd.hex()},
                    timeout=45,
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 200:
                    data = response.json().get("data", "")
                    apdu_result[0] = (
                        bytes.fromhex(data) if data else bytes.fromhex("6985")
                    )
                    print(f"APDU completed: {data}", flush=True)
                else:
                    apdu_result[0] = bytes.fromhex("6985")
                    print(f"APDU HTTP error: {response.status_code}", flush=True)
            except requests.exceptions.Timeout:
                apdu_result[0] = bytes.fromhex("6408")
                print("APDU request timed out", flush=True)
            except Exception as e:
                apdu_result[0] = bytes.fromhex("6985")
                print(f"APDU request failed: {e}", flush=True)
            finally:
                apdu_complete.set()

        def user_interaction():
            try:
                time.sleep(3)
                btn_client = self._get_button_client()
                if not btn_client.connect():
                    print("Failed to connect to buttons", flush=True)
                    return
                print("Starting address confirmation...", flush=True)
                btn_client.press_right()
                time.sleep(2)
                btn_client.press_right()
                time.sleep(1)
                btn_client.press_both()
                time.sleep(2)

                if (
                    apdu_complete.is_set()
                    and apdu_result[0]
                    and apdu_result[0][-2:] == b"\x90\x00"
                ):
                    print("Success on first attempt", flush=True)
                    btn_client.disconnect()
                    return

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
                print("Address confirmation completed", flush=True)
            except Exception as e:
                print(f"User interaction failed: {e}", flush=True)

        threading.Thread(target=send_apdu, daemon=True).start()
        threading.Thread(target=user_interaction, daemon=True).start()

        if apdu_complete.wait(timeout=40) and apdu_result[0]:
            print("Address request completed", flush=True)
            return ExchangeReply(reply=apdu_result[0])

        print("Address request failed or timed out", flush=True)
        return ExchangeReply(reply=bytes.fromhex("6985"))


def main():
    print("Starting Ledger Simulator with gRPC Bridge...", flush=True)
    speculos_process = start_speculos()
    if not speculos_process:
        print("Failed to start Speculos", flush=True)
        sys.exit(1)

    print("Waiting for Speculos to initialize...", flush=True)
    grpc_port = int(os.getenv("GRPC_SERVER_PORT", "3002"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = SpeculosGRPCBridge()
    add_ZemuCommandServicer_to_server(servicer, server)

    listen_addr = f"[::]:{grpc_port}"
    server.add_insecure_port(listen_addr)
    server.start()

    print(
        f"gRPC bridge started at {listen_addr}, ready to forward commands.", flush=True
    )
    print("Manual button interaction enabled for GET_ADDRESS.", flush=True)

    def shutdown(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...", flush=True)
        server.stop(grace=5)
        if speculos_process.poll() is None:
            print("Stopping Speculos...", flush=True)
            speculos_process.terminate()
            try:
                speculos_process.wait(timeout=10)
                print("Speculos stopped", flush=True)
            except subprocess.TimeoutExpired:
                print("Force killing Speculos", flush=True)
                speculos_process.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            if speculos_process.poll() is not None:
                print("Speculos process ended unexpectedly", flush=True)
                break
            time.sleep(30)
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    print("=" * 50, flush=True)
    print("Ledger Simulator Starting", flush=True)
    print("=" * 50, flush=True)
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        import traceback

        traceback.print_exc()
        sys.exit(1)
