import io
import os
import socket
import tarfile
import time
import uuid

import docker
import grpc

from .ledger_utils import ZEMU_API_PORT, ZEMU_BUTTON_PORT, ZEMU_GRPC_SERVER_PORT

ZEMU_IMAGE = "zondax/builder-zemu:speculos-ef9610662dc90a1eeddba7c991e3ef7c53c4e258"


class Ledger:
    def __init__(self, elf_file="app_cosmos.elf", model="nanos", seed=None):
        self.name = f"ledger_simulator_{uuid.uuid4().hex[:8]}"
        self.client = docker.from_env()
        self.elf_file = elf_file
        self.model = model
        self.seed = seed
        self.container = None

    def _pull_image(self):
        try:
            self.client.images.get(ZEMU_IMAGE)
        except docker.errors.ImageNotFound:
            print(f"Pulling image {ZEMU_IMAGE}")
            self.client.images.pull(ZEMU_IMAGE)

    def _cleanup(self):
        try:
            existing = self.client.containers.get(self.name)
            existing.remove(force=True)
        except docker.errors.NotFound:
            pass
        except Exception as e:
            print(f"Cleanup error on {self.name}: {e}")

    def wait_for_grpc_server(self, port=ZEMU_GRPC_SERVER_PORT, timeout=60):
        for i in range(timeout):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(1)
                    if sock.connect_ex(("127.0.0.1", port)) == 0:
                        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
                        grpc.channel_ready_future(channel).result(timeout=5)
                        channel.close()
                        return True
            except Exception:
                pass
            time.sleep(1)
        return False

    def start(self):
        self._pull_image()
        self._cleanup()

        base_path = os.path.dirname(__file__)
        required_files = [
            os.path.join(base_path, "bin", self.elf_file),
            os.path.join(base_path, "entrypoint.py"),
            os.path.join(base_path, "ledger_utils.py"),
        ]

        if not all(os.path.exists(f) for f in required_files):
            raise RuntimeError(f"Required files missing: {required_files}")

        self.container = self.client.containers.create(
            image=ZEMU_IMAGE,
            command=["python3", "/tmp/entrypoint.py"],
            name=self.name,
            ports={
                f"{ZEMU_API_PORT}/tcp": ZEMU_API_PORT,
                f"{ZEMU_GRPC_SERVER_PORT}/tcp": ZEMU_GRPC_SERVER_PORT,
                f"{ZEMU_BUTTON_PORT}/tcp": ZEMU_BUTTON_PORT,
            },
            environment={
                "LEDGER_BINARY": self.elf_file,
                "LEDGER_MODEL": self.model,
                "LEDGER_SEED": self.seed,
                "PYTHONPATH": "/tmp",
                "PYTHONUNBUFFERED": "1",
            },
            entrypoint=[],
            detach=True,
        )

        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            tar.add(required_files[0], arcname=self.elf_file)
            for filepath in required_files[1:]:
                info = tarfile.TarInfo(name=os.path.basename(filepath))
                info.size = os.path.getsize(filepath)
                info.mode = 0o755
                info.mtime = int(time.time())
                with open(filepath, "rb") as f:
                    tar.addfile(info, f)
        tar_stream.seek(0)
        self.container.put_archive("/tmp/", tar_stream.getvalue())
        self.container.start()

        if not self._wait_ready(timeout=60):
            logs = self.container.logs().decode()
            raise RuntimeError(f"Container failed to start:\n{logs}")

        if not self.wait_for_grpc_server():
            raise RuntimeError("Ledger gRPC server failed to start")

        print(f"Ledger container '{self.name}' started successfully")
        return True

    def _wait_ready(self, timeout=30):
        start = time.time()
        while time.time() - start < timeout:
            self.container.reload()
            if self.container.status == "running":
                return True
            time.sleep(1)
        return False

    def stop(self):
        if not self.container:
            return
        try:
            logs = self.container.logs().decode()
            if logs:
                print(f"=== {self.name} logs (last 1000 chars) ===\n{logs[-1000:]}")
            self.container.stop(timeout=10)
            self.container.remove(force=True)
        except docker.errors.NotFound:
            pass
        except Exception as e:
            print(f"Error stopping container {self.name}: {e}")
        self.container = None

    def is_running(self):
        if not self.container:
            return False
        try:
            self.container.reload()
            return self.container.status == "running"
        except (docker.errors.NotFound, Exception):
            return False
