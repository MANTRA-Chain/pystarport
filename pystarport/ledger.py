import io
import os
import socket
import tarfile
import time
import uuid

import docker

ZEMU_HOST = "127.0.0.1"
ZEMU_GRPC_SERVER_PORT = 3002
ZEMU_API_PORT = 5001
ZEMU_BUTTON_PORT = 1235
ZEMU_IMAGE = "zondax/builder-zemu:speculos-261ece66796d4e8e15d944a5ab7ee35246eb8599"


class Ledger:
    def __init__(self):
        self.name = f"ledger_simulator_{uuid.uuid4().hex[:8]}"
        self.client = docker.from_env()
        self.containers = []
        self.container_objects = {}

    def _pull_image(self):
        try:
            self.client.images.get(ZEMU_IMAGE)
            print(f"Image {ZEMU_IMAGE} already exists")
        except docker.errors.ImageNotFound:
            print(f"Pulling image {ZEMU_IMAGE}")
            self.client.images.pull(ZEMU_IMAGE)

    def _cleanup(self):
        try:
            existing = self.client.containers.get(self.name)
            print(f"Removing existing container {self.name}")
            existing.remove(force=True)
        except docker.errors.NotFound:
            pass
        except Exception as e:
            print(f"Cleanup error on {self.name}: {e}")

    def start(self):
        self._pull_image()
        self._cleanup()

        base_path = os.path.dirname(__file__)
        elf_path = os.path.join(base_path, "bin", "app.elf")
        entrypoint_path = os.path.join(base_path, "entrypoint.py")

        if not (os.path.exists(elf_path) and os.path.exists(entrypoint_path)):
            raise RuntimeError(f"Required files missing: {elf_path}, {entrypoint_path}")

        print(f"Creating container {self.name}")
        container = self.client.containers.create(
            image=ZEMU_IMAGE,
            command=["python3", "/tmp/entrypoint.py"],
            name=self.name,
            ports={
                f"{ZEMU_API_PORT}/tcp": ZEMU_API_PORT,
                f"{ZEMU_GRPC_SERVER_PORT}/tcp": ZEMU_GRPC_SERVER_PORT,
                f"{ZEMU_BUTTON_PORT}/tcp": ZEMU_BUTTON_PORT,
            },
            environment={
                "ZEMU_API_PORT": str(ZEMU_API_PORT),
                "ZEMU_GRPC_SERVER_PORT": str(ZEMU_GRPC_SERVER_PORT),
                "ZEMU_BUTTON_PORT": str(ZEMU_BUTTON_PORT),
                "LEDGER_BINARY": "/tmp/app.elf",
                "PYTHONPATH": "/tmp",
                "PYTHONUNBUFFERED": "1",
            },
            entrypoint=[],
            detach=True,
        )

        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            tar.add(elf_path, arcname="app.elf")
            info = tarfile.TarInfo(name="entrypoint.py")
            info.size = os.path.getsize(entrypoint_path)
            info.mode = 0o755
            info.mtime = int(time.time())
            with open(entrypoint_path, "rb") as f:
                tar.addfile(info, f)
        tar_stream.seek(0)

        container.put_archive("/tmp/", tar_stream.getvalue())
        container.start()

        self.containers.append({"Id": container.id, "Name": self.name})
        self.container_objects[self.name] = container

        try:
            print("processes:\n" + container.exec_run("ps aux").output.decode())
            print("/tmp/ contents:\n" + container.exec_run("ls -la /tmp/").output.decode())
            print("python3 version: " + container.exec_run("python3 --version").output.decode().strip())
        except Exception as e:
            print(f"Debug info retrieval failed: {e}")

        if not self._wait_ready(container.id, timeout=60):
            logs = container.logs().decode()
            raise RuntimeError(f"Container failed to start, logs:\n{logs}")

        print("Ledger container started successfully")
        print(f"  Container: {self.name}")
        print(f"  gRPC: 127.0.0.1:{ZEMU_GRPC_SERVER_PORT}")
        print(f"  REST API: http://127.0.0.1:{ZEMU_API_PORT}")

    def _wait_ready(self, container_id, timeout=30):
        start = time.time()
        while time.time() - start < timeout:
            try:
                container = self.client.containers.get(container_id)
                if container.status == "running":
                    print(f"Container {container_id[:12]} is running")
                    return True
            except docker.errors.NotFound:
                print(f"Container {container_id[:12]} not found")
                return False
            time.sleep(1)
        print(f"Container {container_id[:12]} not ready after {timeout}s")
        return False

    def stop(self):
        for c_info in self.containers:
            cid = c_info["Id"]
            cname = c_info.get("Name", cid[:12])
            print(f"Stopping container {cname}")
            try:
                container = self.client.containers.get(cid)
                logs = container.logs().decode()
                if logs:
                    print(f"=== {cname} logs (last 1000 chars) ===\n{logs[-1000:]}")
                container.stop(timeout=10)
                container.remove(force=True)
                print(f"Removed container {cname}")
            except docker.errors.NotFound:
                print(f"Container {cname} already removed")
            except Exception as e:
                print(f"Error stopping container {cname}: {e}")

        self.containers.clear()
        self.container_objects.clear()

    def is_running(self):
        if not self.containers:
            return False
        try:
            container = self.client.containers.get(self.containers[0]["Id"])
            return container.status == "running"
        except docker.errors.NotFound:
            return False
        except Exception as e:
            print(f"Error checking container status: {e}")
            return False


class LedgerButton:
    def __init__(self, address=ZEMU_HOST, port=ZEMU_BUTTON_PORT):
        self.address = address
        self.port = port
        self._client = None
        self.connected = False

    def connect(self):
        if self.connected:
            return True
        try:
            self._client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._client.connect((self.address, self.port))
            self.connected = True
            print(f"Connected to button TCP at {self.address}:{self.port}", flush=True)
            return True
        except Exception as e:
            print(f"Connect failed: {e}", flush=True)
            return False

    def _send(self, data):
        if not self.connect():
            return False
        try:
            self._client.send(data.encode())
            return True
        except Exception as e:
            print(f"Button press failed: {e}", flush=True)
            return False

    def press_left(self):
        if self._send("Ll"):
            print("Pressed left button", flush=True)
            return True
        return False

    def press_right(self):
        if self._send("Rr"):
            print("Pressed right button", flush=True)
            return True
        return False

    def press_both(self):
        if self._send("LRlr"):
            print("Pressed both buttons", flush=True)
            return True
        return False

    def disconnect(self):
        if self.connected and self._client:
            try:
                self._client.close()
                self.connected = False
                print("Disconnected from button TCP", flush=True)
            except Exception as e:
                print(f"Disconnect failed: {e}", flush=True)
