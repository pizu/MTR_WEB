# modules/trace_lock.py
import os, fcntl

class SingleWriterLock:
    def __init__(self, lockfile: str):
        self.lockfile = lockfile
        self.fd = None
    def acquire(self):
        os.makedirs(os.path.dirname(self.lockfile), exist_ok=True)
        self.fd = os.open(self.lockfile, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # raises if already locked
        os.write(self.fd, b"monitor-writer\n")
    def release(self):
        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None
