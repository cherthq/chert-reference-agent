"""Fsynced owner-only receipts and one-process advisory lock."""
import fcntl
import json
import os
from pathlib import Path
import tempfile


class Journal:
    def __init__(self, path, fingerprint):
        self.path = Path(path)
        self.fingerprint = fingerprint
        self.receipts = {}
        self._lock = None

    def open(self):
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            fd = os.open(str(self.path) + '.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            self._lock = os.fdopen(fd, 'a+')
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.path.exists():
                fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd) as f:
                    stat = os.fstat(f.fileno())
                    if stat.st_uid != os.getuid() or stat.st_mode & 0o077:
                        raise RuntimeError('unsafe journal permissions')
                    data = json.load(f)
                if data.get('schema') != 1 or data.get('fingerprint') != self.fingerprint or not isinstance(data.get('receipts'), dict) or any(not isinstance(k, str) or v is not True for k, v in data['receipts'].items()):
                    raise RuntimeError('journal configuration mismatch')
                self.receipts = data['receipts']
            else:
                self._save()
        except Exception as exc:
            self.close()
            raise RuntimeError('journal unavailable') from exc

    def _save(self):
        fd, name = tempfile.mkstemp(prefix=self.path.name + '.', dir=self.path.parent)
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump({'schema': 1, 'fingerprint': self.fingerprint, 'receipts': self.receipts}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(name, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    def add(self, room):
        self.receipts[room] = True
        self._save()

    def remove(self, room):
        previous = self.receipts.pop(room, None)
        try:
            self._save()
        except Exception:
            if previous:
                self.receipts[room] = previous
            raise

    def close(self):
        if self._lock:
            self._lock.close()
            self._lock = None
