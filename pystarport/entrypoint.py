#!/usr/bin/env python3

import os
import signal
import subprocess
import sys
import threading
import time
from concurrent import futures


def ensure_deps_installed():
    def install_package(package_names):
        try:
            res = subprocess.run(
                [sys.executable, "-m", "pip", "install", *package_names, "--quiet"],
                capture_output=True,
                text=True,
                timeout=120,
            )
            return res.returncode == 0, res.stderr
        except Exception as e:
            return False, str(e)

    try:
        import grpc

        grpc_available = True
    except ImportError:
        success, error = install_package(["grpcio", "grpcio-tools"])
        if success:
            import grpc

            grpc_available = True
        else:
            print(f"Failed to install grpcio: {error}", flush=True)
            grpc_available = False

    try:
        from ledger_utils import LedgerButton

        ledger_button_available = True
    except ImportError as e:
        print(f"Failed to import LedgerButton: {e}", flush=True)
        ledger_button_available = False

    if not grpc_available:
        print("grpcio not available", flush=True)

    if not ledger_button_available:
        print("LedgerButton not available", flush=True)
        return False

    return True


if not ensure_deps_installed():
    print("Failed to load required dependencies", flush=True)
    sys.exit(1)

import grpc
from ledger_utils import (
    STATUS_GENERAL_ERROR,
    STATUS_WRONG_LENGTH,
    ZEMU_API_PORT,
    ZEMU_BUTTON_PORT,
    ZEMU_GRPC_SERVER_PORT,
    LedgerAPDU,
    LedgerButton,
    cosmos_address_automation,
    ethereum_transaction_automation,
)


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
    ledger_binary = f"/tmp/{os.getenv('LEDGER_BINARY', 'app_cosmos.elf')}"
    if not os.path.exists(ledger_binary):
        print(f"Ledger binary not found at {ledger_binary}", flush=True)
        return None

    ledger_model = os.getenv("LEDGER_MODEL", "nanos")
    ledger_seed = os.getenv("LEDGER_SEED")
    speculos_cmd = [
        "/home/zondax/speculos/speculos.py",
        "--model",
        ledger_model,
        ledger_binary,
        *(["--seed", ledger_seed] if ledger_seed else []),
        "--display",
        "headless",
        "--apdu-port",
        os.getenv("ZEMU_APDU_PORT", "9999"),
        "--api-port",
        str(ZEMU_API_PORT),
        "--button-port",
        str(ZEMU_BUTTON_PORT),
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
        return proc
    except Exception as e:
        print(f"Failed to start Speculos: {e}", flush=True)
        return None


class SpeculosGRPCBridge:
    def __init__(self):
        self.apdu_client = LedgerAPDU(ZEMU_API_PORT)
        self._test_speculos_connection()

    def _test_speculos_connection(self):
        for attempt in range(10):
            try:
                import requests

                resp = requests.get(f"http://127.0.0.1:{ZEMU_API_PORT}/", timeout=5)
                if resp.status_code == 200:
                    test_response = self.apdu_client.send_apdu("B001000000", timeout=10)
                    if LedgerAPDU.is_success(test_response):
                        return True
            except Exception as e:
                print(f"Connection test error: {e}", flush=True)
            time.sleep(2)
        print("Failed to connect to Speculos after all retries", flush=True)
        return False

    def Exchange(self, request, context):
        try:
            cmd = request.command
            cmd_hex = cmd.hex()

            if len(cmd) >= 2:
                cla, ins = cmd[0], cmd[1]

                if cla == 0xE0:
                    if ins == 0x02:
                        return self._handle_eth_address_request(cmd)
                    elif ins == 0x04:
                        return self._handle_eth_transaction_signing(cmd)
                elif cla == 0x55:
                    if ins == 0x04:
                        return self._handle_cosmos_address_request(cmd)
                    elif ins == 0x02:
                        return self._handle_cosmos_transaction_signing(cmd)

            # Default: send to Speculos
            response_bytes = self.apdu_client.send_apdu(cmd_hex, timeout=30)
            return ExchangeReply(reply=response_bytes)

        except Exception as e:
            print(f"gRPC Exchange error: {e}", flush=True)
            return ExchangeReply(reply=STATUS_GENERAL_ERROR)

    def _handle_eth_address_request(self, cmd):
        if len(cmd) < 4:
            return ExchangeReply(reply=STATUS_WRONG_LENGTH)

        # Try silent mode first
        if len(cmd) >= 3 and cmd[2] == 0x01:
            silent_cmd = bytearray(cmd)
            silent_cmd[2] = 0x00
            response_bytes = self.apdu_client.send_apdu(silent_cmd.hex(), timeout=10)
            if LedgerAPDU.is_success(response_bytes):
                return ExchangeReply(reply=response_bytes)

        return self._handle_eth_interactive_address(cmd)

    def _handle_eth_interactive_address(self, cmd):
        btn_client = LedgerButton()

        def automation(apdu_complete):
            try:
                time.sleep(2)
                if not btn_client.connect():
                    return
                time.sleep(3)
                if not apdu_complete.is_set():
                    btn_client.press_both()
                    time.sleep(2)
                btn_client.disconnect()
            except Exception:
                pass

        response_bytes = self.apdu_client.send_apdu_with_automation(
            cmd.hex(), automation, timeout=40
        )

        return ExchangeReply(reply=response_bytes)

    def _handle_eth_transaction_signing(self, cmd):
        btn_client = LedgerButton()

        def automation(apdu_complete):
            ethereum_transaction_automation(btn_client, apdu_complete)

        response_bytes = self.apdu_client.send_apdu_with_automation(
            cmd.hex(), automation, timeout=100
        )

        return ExchangeReply(reply=response_bytes)

    def _handle_cosmos_address_request(self, cmd):
        # Try silent mode first
        silent_cmd = bytearray(cmd)
        silent_cmd[2] = 0x00
        response_bytes = self.apdu_client.send_apdu(silent_cmd.hex(), timeout=10)
        if LedgerAPDU.is_success(response_bytes):
            return ExchangeReply(reply=response_bytes)

        # Interactive mode
        btn_client = LedgerButton()

        def automation(apdu_complete):
            cosmos_address_automation(btn_client, apdu_complete)

        response_bytes = self.apdu_client.send_apdu_with_automation(
            cmd.hex(), automation, timeout=40
        )

        return ExchangeReply(reply=response_bytes)

    def _handle_cosmos_transaction_signing(self, cmd):
        p1, p2 = cmd[2], cmd[3] if len(cmd) >= 4 else (0, 0)

        if p1 == 0x02:
            return self._handle_final_transaction_approval(cmd)

        # Non-final signing steps
        response_bytes = self.apdu_client.send_apdu(cmd.hex(), timeout=10)
        return ExchangeReply(reply=response_bytes)

    def _handle_final_transaction_approval(self, cmd):
        btn_client = LedgerButton()

        def automation(apdu_complete):
            try:
                time.sleep(3)
                if not btn_client.connect():
                    return

                # Navigate through screens
                for i in range(11):
                    if apdu_complete.is_set():
                        break
                    btn_client.press_right()
                    time.sleep(2)

                # Approval attempts
                if not apdu_complete.is_set():
                    for attempt in range(6):
                        if apdu_complete.is_set():
                            break
                        btn_client.press_both()
                        time.sleep(2.5)

                        if attempt == 2 and not apdu_complete.is_set():
                            btn_client.press_left()
                            time.sleep(1.5)
                        elif attempt == 4 and not apdu_complete.is_set():
                            btn_client.press_left()
                            time.sleep(1)

                btn_client.disconnect()
            except Exception:
                pass

        response_bytes = self.apdu_client.send_apdu_with_automation(
            cmd.hex(), automation, timeout=60
        )

        return ExchangeReply(reply=response_bytes)


def main():
    speculos_process = start_speculos()
    if not speculos_process:
        sys.exit(1)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = SpeculosGRPCBridge()
    add_ZemuCommandServicer_to_server(servicer, server)

    listen_addr = f"[::]:{ZEMU_GRPC_SERVER_PORT}"
    server.add_insecure_port(listen_addr)
    server.start()

    def shutdown(signum, frame):
        server.stop(grace=5)
        if speculos_process.poll() is None:
            speculos_process.terminate()
            try:
                speculos_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                speculos_process.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        while True:
            if speculos_process.poll() is not None:
                break
            time.sleep(30)
    except KeyboardInterrupt:
        shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}", flush=True)
        sys.exit(1)
