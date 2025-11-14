import os
import sys
import tempfile
from pathlib import Path

if not os.environ.get("TMPDIR", "").startswith("/tmp"):
    os.environ["TMPDIR"] = "/tmp"
    tempfile.tempdir = "/tmp"

proto_folder = Path(os.path.abspath(__file__)).parent.joinpath("proto_python")
sys.path.append(str(proto_folder))
