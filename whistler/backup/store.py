"""The backup volume's directory: one file per backup, nothing else.

The file name is the index (``schedule.FILENAME_RE``: when, and why), and the
manifest inside is the rest. Nothing else is stored beside them, so the
directory is self-describing: copy it anywhere and it is still a list of
backups. The name pattern is also the path validation — a name that does not
match is not a file this store will open, write or delete.
"""

import datetime
import os
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from whistler.backup import BackupError, archive, schedule


class Store:
    def __init__(self, directory: str):
        self.dir = Path(directory)
        self._cache: Dict[str, tuple] = {}
        self._lock = threading.Lock()

    def _path(self, name: str) -> Path:
        if not schedule.parse_filename(name):
            raise BackupError(f"Not a backup file name: {name!r}")
        return self.dir / name

    def _entry(self, path: Path) -> Optional[Dict[str, Any]]:
        parsed = schedule.parse_filename(path.name)
        if not parsed:
            return None
        st = path.stat()
        key = (st.st_mtime_ns, st.st_size)
        cached = self._cache.get(path.name)
        if cached and cached[0] == key:
            return cached[1]
        try:
            manifest = archive.read_manifest(path.read_bytes())
            problem = None
        except BackupError as e:
            manifest, problem = {}, str(e)
        created = parsed["stamp"]
        try:
            created = datetime.datetime.strptime(
                manifest["createdAt"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=datetime.timezone.utc)
        except (KeyError, TypeError, ValueError):
            pass
        entry = {"file": path.name, "size": st.st_size, "createdAt": created,
                 # From the name, not the manifest: an uploaded backup keeps
                 # its original manifest but is "uploaded" here.
                 "trigger": parsed["trigger"],
                 "installId": manifest.get("installId"),
                 "contentHash": manifest.get("contentHash"),
                 "manifest": manifest, "problem": problem}
        self._cache[path.name] = (key, entry)
        return entry

    def list(self) -> List[Dict[str, Any]]:
        """Every backup, newest first. Unreadable ones are listed with a
        ``problem`` rather than hidden: an admin should see a damaged file."""
        if not self.dir.is_dir():
            raise BackupError(f"The backup volume is not mounted at {self.dir}.")
        entries = [e for e in (self._entry(p) for p in self.dir.iterdir()
                               if p.is_file()) if e]
        return sorted(entries, key=lambda e: e["createdAt"], reverse=True)

    def save(self, data: bytes, created: datetime.datetime,
             trigger: str) -> Dict[str, Any]:
        """Write a backup atomically: a temporary file in the same directory,
        fsync'd, then renamed. A crash leaves the old state or the new file,
        never a half-written backup under a backup's name."""
        with self._lock:
            taken = {p.name for p in self.dir.iterdir()}
            name = schedule.filename_for(created, trigger, taken)
            fd, tmp = tempfile.mkstemp(dir=self.dir, prefix=".incoming-")
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.chmod(tmp, 0o600)
                os.replace(tmp, self.dir / name)
            except BaseException:
                try:
                    os.unlink(tmp)
                except FileNotFoundError:
                    pass
                raise
            dfd = os.open(self.dir, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        return self._entry(self.dir / name)

    def read(self, name: str) -> bytes:
        path = self._path(name)
        if not path.is_file():
            raise BackupError(f"No backup {name}.")
        return path.read_bytes()

    def delete(self, name: str) -> None:
        path = self._path(name)
        try:
            path.unlink()
        except FileNotFoundError:
            raise BackupError(f"No backup {name}.")
        self._cache.pop(name, None)

    def usage(self) -> Dict[str, int]:
        total, used, free = shutil.disk_usage(self.dir)
        return {"capacityBytes": total, "usedBytes": used, "freeBytes": free}

    def sweep_incoming(self) -> None:
        """Remove temporaries a crash left behind."""
        for p in self.dir.glob(".incoming-*"):
            try:
                p.unlink()
            except OSError:
                pass
