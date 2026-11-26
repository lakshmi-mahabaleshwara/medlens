import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from medlens.common import writer_lock


def test_cross_process_workspace_lock():
    # Use the same lock location and type as the normal Compose writer.
    root = Path("outputs")
    root.mkdir(exist_ok=True)
    with TemporaryDirectory(dir=root) as directory:
        path = Path(directory).resolve()
        with writer_lock(path) as lockfile:
            probe = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import fcntl,sys; lock=open(sys.argv[1], 'r+b'); "
                    "fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)",
                    str(lockfile),
                ],
                capture_output=True,
                text=True,
            )
            assert probe.returncode and "BlockingIOError" in probe.stderr
        acquired = subprocess.run(
            [
                sys.executable,
                "-c",
                "from pathlib import Path;from medlens.common import writer_lock;"
                "import sys\nwith writer_lock(Path(sys.argv[1])): print('acquired')",
                str(path),
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        assert acquired.stdout.strip() == "acquired"
