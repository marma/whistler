import asyncio
import asyncssh
import sys
import stat

class DummySFTPFile:
    def __init__(self, sftp_server, path, flags, attrs):
        self._pos = 0

    def write(self, data):
        print(f"SFTP: write(len={len(data)}, pos={self._pos})", file=sys.stderr, flush=True)
        self._pos += len(data)
        return len(data)

    def writev(self, chunks, offset):
        print(f"SFTP: writev({len(chunks)} chunks, offset={offset})", file=sys.stderr, flush=True)
        total_len = 0
        for data in chunks:
            total_len += len(data)
        self._pos = offset + total_len
        return total_len

    def seek(self, offset, whence=0):
        print(f"SFTP: seek(offset={offset}, whence={whence})", file=sys.stderr, flush=True)
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        return self._pos

    def tell(self):
        return self._pos

    def setstat(self, attrs):
        print(f"SFTP: file.setstat(attrs={attrs})", file=sys.stderr, flush=True)
        return None

    def truncate(self, size):
        print(f"SFTP: truncate(size={size})", file=sys.stderr, flush=True)
        return size

    def read(self, size, offset):
        print(f"SFTP: read(size={size}, offset={offset})", file=sys.stderr, flush=True)
        return b''

    def close(self):
        print(f"SFTP: close()", file=sys.stderr, flush=True)
        return None

class DummySFTPServer(asyncssh.SFTPServer):
    async def open(self, path, pflags, attrs):
        print(f"SFTP: open(path={path}, pflags={pflags})", file=sys.stderr, flush=True)
        return DummySFTPFile(self, path, pflags, attrs)

    async def fstat(self, file_obj):
        print(f"SFTP: fstat(file_obj={file_obj})", file=sys.stderr, flush=True)
        attrs = asyncssh.SFTPAttrs()
        attrs.size = getattr(file_obj, '_pos', 0)
        attrs.permissions = stat.S_IFREG | 0o644
        return attrs

    async def fsetstat(self, file_obj, attrs):
        print(f"SFTP: fsetstat(file_obj={file_obj}, attrs={attrs})", file=sys.stderr, flush=True)
        if hasattr(file_obj, 'setstat'):
            file_obj.setstat(attrs)
        return None

    async def stat(self, path):
        print(f"SFTP: stat(path={path})", file=sys.stderr, flush=True)
        attrs = asyncssh.SFTPAttrs()
        # Pretend any file is a 0-byte regular file
        attrs.size = 0
        attrs.permissions = stat.S_IFREG | 0o644
        return attrs

    async def realpath(self, path):
        print(f"SFTP: realpath(path={path})", file=sys.stderr, flush=True)
        return path

    async def lstat(self, path):
        return await self.stat(path)

    async def setstat(self, path, attrs):
        print(f"SFTP: setstat(path={path}, attrs={attrs})", file=sys.stderr, flush=True)
        return None

class DummySSHServer(asyncssh.SSHServer):
    def begin_auth(self, username):
        # Allow any user
        return False

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        return True

    def public_key_auth_supported(self):
        return True

    def validate_public_key(self, username, key):
        return True

async def start_server():
    host_key = asyncssh.generate_private_key('ssh-rsa')
    
    server = await asyncssh.create_server(
        DummySSHServer, '', 2222,
        server_host_keys=[host_key],
        sftp_factory=DummySFTPServer
    )
    
    print("SSH Server started on port 2222...")
    await server.wait_closed()

if __name__ == '__main__':
    try:
        asyncio.run(start_server())
    except (OSError, asyncssh.Error) as exc:
        sys.exit(f'Error starting server: {exc}')
    except KeyboardInterrupt:
        pass
