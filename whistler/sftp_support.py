import asyncssh
import asyncio
import sys
import time
import stat
from whistler.globals import CONN_SERVER_MAP as _CONN_SERVER_MAP

class WhistlerSFTPFile:
    """SFTP File Handle that streams via kubectl exec."""
    def __init__(self, sftp_server, path, flags, attrs):
        self.sftp_server = sftp_server
        self.path = path
        self.flags = flags
        self.attrs = attrs
        self.pos = 0
        self.proc = None
        self._closing = False
        
    async def read(self, size, offset):
        if self._closing:
            raise asyncssh.SFTPError(asyncssh.FX_BAD_MESSAGE, "File is closed")
        if not self.sftp_server.pod_name:
            raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No active pod found")
        
        # Restart reader if seeking to new position
        if self.proc and offset != self.pos:
            await self.close()
            
        if not self.proc:
            cmd = ["kubectl", "exec", "-i", self.sftp_server.pod_name, 
                   "-n", self.sftp_server.namespace, "--",
                   "tail", "-c", f"+{int(offset)+1}", self.path]
            try:
                self.proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            except Exception as e:
                raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))
            self.pos = offset
        
        try:
            data = await self.proc.stdout.read(size)
            self.pos += len(data)
            return data
        except Exception as e:
            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))

    async def write(self, data, offset):
        if self._closing:
            raise asyncssh.SFTPError(asyncssh.FX_BAD_MESSAGE, "File is closed")
        if not self.sftp_server.pod_name:
            raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No active pod found")
        
        # Only support sequential writes
        if self.proc and offset != self.pos:
            await self.close()
        
        if not self.proc:
            shell_op = ">" if offset == 0 else ">>"
            cmd = ["kubectl", "exec", "-i", self.sftp_server.pod_name,
                   "-n", self.sftp_server.namespace, "--",
                   "sh", "-c", f"cat {shell_op} {self.path}"]
            try:
                self.proc = await asyncio.create_subprocess_exec(
                    *cmd, stdin=asyncio.subprocess.PIPE, 
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            except Exception as e:
                raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))
            self.pos = offset
        
        try:
            self.proc.stdin.write(data)
            await self.proc.stdin.drain()
            self.pos += len(data)
        except Exception as e:
            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))

    async def close(self):
        self._closing = True
        if self.proc:
            try:
                if self.proc.stdin:
                    self.proc.stdin.close()
                self.proc.terminate()
                await self.proc.wait()
            except Exception:
                pass
            self.proc = None

    async def stat(self):
        return await self.sftp_server.stat(self.path)
        
    async def setstat(self, attrs):
        pass  # Ignore for now


class WhistlerSFTPServer(asyncssh.SFTPServer):
    """SFTP Server that translates operations to kubectl exec commands."""
    def __init__(self, chan):
        super().__init__(chan)
        self._chan = chan
        conn = chan.get_connection()
        self._server = _CONN_SERVER_MAP.get(conn)
        if not self._server:
            raise RuntimeError("SSHServer instance not found for connection")
        
        self.username = self._server.username
        self.target_name = getattr(self._server, 'active_instance_name', None) or self._server.target_name
        self.config_manager = self._server.config_manager
        self.pod_name = None
        self.namespace = self.config_manager.namespace
        
    async def _ensure_pod(self):
        """Ensure target pod is running, start if necessary."""
        if self.pod_name:
            return
        if not self.target_name:
            raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No target instance specified")
        
        instances = self.config_manager.get_user_instances(self.username)
        instance = next((i for i in instances if i["name"] == self.target_name), None)
        
        if not instance:
            raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, 
                f"Instance {self.target_name} not found. Please create it first.")
        
        self.namespace = instance.get("namespace", self.namespace)
        
        # Start instance if not running
        if instance.get("status") != "Running" or not instance.get("podName"):
            try:
                full_cr_name = f"{self.username}-{self.target_name}"
                self.config_manager.api.patch_namespaced_custom_object(
                    self.config_manager.group, self.config_manager.version, self.namespace,
                    "whistlerinstances", full_cr_name,
                    {"metadata": {"annotations": {"whistler/last-connect": str(time.time())}}})
            except Exception as e:
                raise asyncssh.SFTPError(asyncssh.FX_FAILURE, f"Failed to start instance: {e}")
            
            # Wait for running (60s timeout)
            start_time = time.time()
            while time.time() - start_time < 60:
                instances = self.config_manager.get_user_instances(self.username)
                instance = next((i for i in instances if i["name"] == self.target_name), None)
                if instance and instance.get("status") == "Running" and instance.get("podName"):
                    break
                await asyncio.sleep(1)
            
            if not instance or instance.get("status") != "Running":
                raise asyncssh.SFTPError(asyncssh.FX_CONNECTION_LOST, 
                    "Timed out waiting for instance to start")
        
        self.pod_name = instance.get("podName")

    async def _exec(self, cmd_args):
        """Execute command in pod via kubectl."""
        await self._ensure_pod()
        full_cmd = ["kubectl", "exec", self.pod_name, "-n", self.namespace, "--"] + cmd_args
        proc = await asyncio.create_subprocess_exec(
            *full_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            err_msg = stderr.decode()
            if "No such file" in err_msg:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, "File not found")
            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, err_msg)
        return stdout.decode().strip()

    async def stat(self, path):
        try:
            res = await self._exec(["stat", "-c", "%F|%s|%Y|%f", path])
            parts = res.split("|")
            if len(parts) != 4:
                raise ValueError("Invalid stat output")
            
            ftype_str, size, mtime, raw_mode = parts
            kind = stat.S_IFREG
            if "directory" in ftype_str.lower():
                kind = stat.S_IFDIR
            
            attrs = asyncssh.SFTPAttrs()
            attrs.size = int(size)
            attrs.mtime = int(mtime)
            attrs.atime = int(mtime)
            attrs.permissions = int(raw_mode, 16)
            return attrs
        except asyncssh.SFTPError:
            raise
        except Exception as e:
            raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, str(e))

    async def lstat(self, path):
        return await self.stat(path)

    async def open(self, path, pflags, attrs):
        await self._ensure_pod()
        # Handle file creation/truncation
        if pflags & asyncssh.FXF_CREAT:
            try:
                exists = True
                try:
                    await self.stat(path)
                except asyncssh.SFTPError:
                    exists = False
                if not exists or (pflags & asyncssh.FXF_TRUNC):
                    await self._exec(["sh", "-c", f"cat /dev/null > {path}"])
            except Exception:
                pass  # Non-fatal
        return WhistlerSFTPFile(self, path, pflags, attrs)

    async def listdir(self, path):
        try:
            output = await self._exec(["ls", "-ln", "--time-style=+%s", path])
        except Exception:
            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, "Failed to list directory")
        
        entries = []
        for line in output.splitlines():
            if line.startswith("total"):
                continue
            parts = line.split()
            if len(parts) < 8:
                continue
            perms, filename = parts[0], " ".join(parts[6:])
            if filename in (".", ".."):
                continue
            
            attrs = asyncssh.SFTPAttrs()
            try:
                attrs.size = int(parts[4])
                attrs.mtime = int(parts[5])
                attrs.atime = int(parts[5])
                if perms.startswith('d'):
                    attrs.permissions = 0o755 | stat.S_IFDIR
                else:
                    attrs.permissions = 0o644 | stat.S_IFREG
            except:
                pass
            entries.append((filename, attrs))
        return entries
        
    async def mkdir(self, path, attrs):
        await self._exec(["mkdir", "-p", path])
        
    async def rmdir(self, path):
        await self._exec(["rmdir", path])
        
    async def remove(self, path):
        await self._exec(["rm", path])
        
    async def rename(self, oldpath, newpath):
        await self._exec(["mv", oldpath, newpath])

