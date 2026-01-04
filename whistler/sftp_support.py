import asyncssh
import asyncio
import sys
import time
import stat
from whistler.globals import CONN_SERVER_MAP as _CONN_SERVER_MAP

class WhistlerSFTPFile:
    """SFTP File Handle that streams via kubectl exec."""
    def __init__(self, sftp_server, path, flags, attrs):
        print(f"SFTP: WhistlerSFTPFile created for {path} (flags={flags}, attrs={attrs})", file=sys.stderr, flush=True)
        self.sftp_server = sftp_server
        self.path = sftp_server._norm_path(path)
        self.flags = flags
        self.attrs = attrs
        self.pos = 0
        self.proc = None
        self._closing = False
        self._write_lock = asyncio.Lock()
    
    def fileno(self):
        raise OSError("No file descriptor")

    def __getattr__(self, name):
         # Catch access to missing methods/attributes
        print(f"SFTP: WhistlerSFTPFile missing attr access: {name}", file=sys.stderr, flush=True)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    async def writev(self, chunks, offset):
        print(f"SFTP: writev({len(chunks)} chunks, offset={offset})", file=sys.stderr, flush=True)
        # Fallback to write (inefficient but safe)
        length = 0
        for data in chunks:
            await self.write(data, offset + length)
            length += len(data)
        return length

    def seek(self, offset, whence=0):
        print(f"SFTP: seek({offset}, whence={whence})", file=sys.stderr, flush=True)
        if whence == 0:
            self.pos = offset
        elif whence == 1:
            self.pos += offset
        elif whence == 2:
            raise OSError("Seek from end not supported synchronously")
        return self.pos

    def tell(self):
        return self.pos

    def flush(self):
        # We don't buffer explicitly, but we should clear our subprocess if it's done?
        # Or just no-op as 'write' awaits its completion.
        pass
        
    async def read(self, size, offset):
        print(f"SFTP: read(size={size}, offset={offset})", file=sys.stderr, flush=True)
        if self._closing:
            raise asyncssh.SFTPError(asyncssh.FX_BAD_MESSAGE, "File is closed")
        await self.sftp_server._ensure_pod()
        if not self.sftp_server.pod_name:
            raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No active pod found")
        
        # Restart reader if seeking to new position
        if self.proc and offset != self.pos:
            await self.close_async()
            
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
        # logging at top to verify call
        print(f"SFTP: write(len={len(data)}, offset={offset})", file=sys.stderr, flush=True)
        
        if self._closing:
            raise asyncssh.SFTPError(asyncssh.FX_BAD_MESSAGE, "File is closed")
            
        async with self._write_lock:
            # Retry loop (max 1 retry)
            for attempt in range(2):
                try:
                    await self.sftp_server._ensure_pod()
                    if not self.sftp_server.pod_name:
                        raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No active pod found")
                    
                    # Restart writer if seeking to new position or if it's the first write
                    if self.proc and offset != self.pos:
                        print(f"SFTP: offset mismatch (req={offset}, current={self.pos}), restarting writer", file=sys.stderr, flush=True)
                        await self._stop_process()
                    
                    print(self.proc, file=sys.stderr, flush=True)
                    if not self.proc:
                        # Use truncate to ensure we append at the exact correct offset
                        # This handles retries (where file might be larger or smaller than offset)
                        # and ensures we don't need slow 'dd bs=1'.
                        
                        # Note: we quote path strictly.
                        # If offset=0, we can just use > to truncate.
                        if offset == 0:
                            shell_cmd = f"(cat > '{self.path}')"
                        else:
                            # truncate -s SIZE ensures file is exactly SIZE bytes.
                            # - If smaller, extends with nulls (holes).
                            # - If larger, truncates.
                            # Then cat >> appends from that point.
                            shell_cmd = f"(truncate -s {offset} '{self.path}' && cat >> '{self.path}')"
                        
                        cmd = ["kubectl", "exec", "-i", self.sftp_server.pod_name,
                               "-n", self.sftp_server.namespace, "--",
                               "sh", "-c", shell_cmd]
                               
                        try:
                            print(f"SFTP: Starting upload process (attempt {attempt+1}): {cmd}", file=sys.stderr, flush=True)
                            self.proc = await asyncio.create_subprocess_exec(
                                *cmd, stdin=asyncio.subprocess.PIPE, 
                                stdout=asyncio.subprocess.DEVNULL, 
                                stderr=asyncio.subprocess.PIPE)
                            
                            async def consume_stderr(proc, path):
                                try:
                                    while True:
                                        line = await proc.stderr.readline()
                                        if not line: break
                                        print(f"SFTP Write Stderr ({path}): {line.decode().strip()}", file=sys.stderr, flush=True)
                                except Exception:
                                    pass
                            asyncio.create_task(consume_stderr(self.proc, self.path))
                            
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            if attempt == 0:
                                continue
                            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))
                        self.pos = offset
                
                    self.proc.stdin.write(data)
                    await self.proc.stdin.drain()
                    self.pos += len(data)
                    break 
                    
                except (BrokenPipeError, ConnectionResetError) as e:
                    print(f"SFTP: write broken pipe (attempt {attempt+1}): {e}", file=sys.stderr, flush=True)
                    await self._stop_process()
                    if attempt == 1:
                        raise asyncssh.SFTPError(asyncssh.FX_FAILURE, f"Write failed after retry: {e}")
                except Exception as e:
                    print(f"SFTP: write failed for {self.path} (attempt {attempt+1}): {e}", file=sys.stderr, flush=True)
                    if attempt == 1:
                        raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))

    def readable(self):
        return True

    def writable(self):
        return True

    def seekable(self):
        return True

    def chmod(self, mode):
        print(f"SFTP: chmod({mode}) on {self.path}", file=sys.stderr, flush=True)
        return None

    def chown(self, uid, gid):
        print(f"SFTP: chown({uid}, {gid}) on {self.path}", file=sys.stderr, flush=True)
        return None
        
    def chgrp(self, gid):
        print(f"SFTP: chgrp({gid}) on {self.path}", file=sys.stderr, flush=True)
        return None

    def utimes(self, atime, mtime):
        print(f"SFTP: utimes({atime}, {mtime}) on {self.path}", file=sys.stderr, flush=True)
        return None

    async def _stop_process(self):
        """Clean up the kubectl process without marking file as closed (unless done by close)."""
        if self.proc:
            # For writing (stdin used), we MUST close stdin and wait for natural exit
            if self.proc.stdin:
                try:
                    self.proc.stdin.close()
                    await self.proc.stdin.wait_closed()
                except Exception as e:
                    # print(f"SFTP: close stdin error: {e}", file=sys.stderr, flush=True)
                    pass
            
            # Wait for process to exit naturally
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                # print(f"SFTP: process didn't exit, terminating...", file=sys.stderr, flush=True)
                try:
                    self.proc.terminate()
                    await asyncio.wait_for(self.proc.wait(), timeout=2.0)
                except Exception:
                    try:
                        self.proc.kill()
                    except:
                        pass
            except Exception:
                pass
                
            self.proc = None

    def close(self):
        """Synchronous close required by asyncssh _cleanup loop."""
        print(f"SFTP: close({self.path})", file=sys.stderr, flush=True)
        if not self._closing:
            self._closing = True
            asyncio.create_task(self.close_async())

    async def close_async(self):
        """Asynchronous cleanup."""
        await self._stop_process()

    async def stat(self):
        return await self.sftp_server.stat(self.path)
        
    def setstat(self, attrs):
        print(f"SFTP: setstat({self.path}, {attrs}) - ignoring", file=sys.stderr, flush=True)
        # Explicitly ignore setstat to satisfy scp -p
        return None

    def truncate(self, size):
        print(f"SFTP: truncate({size}) on {self.path} - ignoring/handled by write", file=sys.stderr, flush=True)
        # We can implement this if needed, but for now just acknowledge.
        # If we really need validation, we can launch async task?
        # For scp upload, it's advisory.
        return size

class WhistlerSFTPServer(asyncssh.SFTPServer):
    """SFTP Server that translates operations to kubectl exec commands."""
    def __init__(self, session):
        print(f"WhistlerSFTPServer initializing with session={session}", file=sys.stderr, flush=True)
        super().__init__(session._chan)
        self._session = session
        self._chan = session._chan
        self.username = session.username
        self.target_type = session.target_type
        # target_name might be a template initially, _ensure_pod will resolve it
        self.target_name = session.target_name
        self.config_manager = session.config_manager
        self.pod_name = None
        self.namespace = self.config_manager.namespace
        self._lock = asyncio.Lock()
        print(f"SFTP context: user={self.username}, target={self.target_name}, type={self.target_type}", file=sys.stderr, flush=True)
        
    def _norm_path(self, path):
        if isinstance(path, bytes):
            return path.decode('utf-8', errors='replace')
        return path
    
    # ... existing methods ...
    
    async def open(self, path, pflags, attrs):
        path = self._norm_path(path)
        print(f"SFTP: open(path={path}, pflags={pflags})", file=sys.stderr, flush=True)
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
                    print(f"SFTP: creating/truncating {path}", file=sys.stderr, flush=True)
                    # Force creation of empty file
                    await self._exec(["sh", "-c", f"(cat > '{path}') < /dev/null"])
            except Exception as e:
                 print(f"SFTP: warning - failed to truncate/create {path}: {e}", file=sys.stderr, flush=True)
        
        f = WhistlerSFTPFile(self, path, pflags, attrs)
        print(f"SFTP: open returning file object {f}", file=sys.stderr, flush=True)
        return f

    async def _ensure_pod(self):
        """Ensure target pod is running, start if necessary."""
        async with self._lock:
            if self.pod_name:
                return
                
            print(f"SFTP: _ensure_pod(user={self.username}, target={self.target_name}, type={self.target_type})", file=sys.stderr, flush=True)
            if not self.username:
                raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No username found in session")

            # Check for existing instance first
            try:
                instances = self.config_manager.get_user_instances(self.username)
            except Exception as e:
                print(f"SFTP Error: Failed to get user instances: {e}", file=sys.stderr, flush=True)
                raise asyncssh.SFTPError(asyncssh.FX_FAILURE, f"Failed to list instances: {e}")

            instance = next((i for i in instances if i["name"] == self.target_name), None)
            
            if not instance and self.target_type == "template":
                # Use the template_name resolved in WhistlerSession
                template_name = getattr(self._session, 'template_name', None) or self.target_name
                print(f"SFTP: Instance {self.target_name} not found, checking template {template_name}", file=sys.stderr, flush=True)
                
                templates = self.config_manager.get_user_templates(self.username)
                template_obj = next((t for t in templates if t["name"] == template_name), None)
                
                if template_obj:
                    template_ref = template_obj.get("fullName", template_name)
                    print(f"SFTP: Creating anonymous instance {self.target_name} from template {template_ref}", file=sys.stderr, flush=True)
                    
                    if not self.config_manager.add_instance(self.username, template_ref, self.target_name, preemptible=True):
                        raise asyncssh.SFTPError(asyncssh.FX_FAILURE, "Failed to create instance from template")
                    
                    # Wait for instance to appear
                    start_time = time.time()
                    while time.time() - start_time < 30:
                        instances = self.config_manager.get_user_instances(self.username)
                        instance = next((i for i in instances if i["name"] == self.target_name), None)
                        if instance:
                             break
                        await asyncio.sleep(1)
                
                if not instance:
                    print(f"SFTP Error: Could not resolve target {self.target_name} to instance or template", file=sys.stderr, flush=True)
                    raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, f"Target {self.target_name} not found")

            if not instance:
                # If target_type was "instance" but not found
                print(f"SFTP Error: Instance {self.target_name} not found", file=sys.stderr, flush=True)
                raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, f"Instance {self.target_name} not found")
            
            self.namespace = instance.get("namespace", f"whistler-user-{self.username}")
            print(f"SFTP: Target instance {self.target_name} status={instance.get('status')} in ns={self.namespace}", file=sys.stderr, flush=True)
            
            # Start instance if not running
            if instance.get("status") != "Running" or not instance.get("podName"):
                try:
                    full_cr_name = f"{self.username}-{self.target_name}"
                    print(f"SFTP: Patching instance {full_cr_name} to start (namespace={self.namespace})...", file=sys.stderr, flush=True)
                    self.config_manager.api.patch_namespaced_custom_object(
                        self.config_manager.group, self.config_manager.version, self.namespace,
                        "whistlerinstances", full_cr_name,
                        {"metadata": {"annotations": {"whistler/last-connect": str(time.time())}}})
                except Exception as e:
                    print(f"SFTP Error: Failed to start instance {full_cr_name}: {e}", file=sys.stderr, flush=True)
                    raise asyncssh.SFTPError(asyncssh.FX_FAILURE, f"Failed to start instance: {e}")
                
                # Wait for running (60s timeout)
                start_time = time.time()
                while time.time() - start_time < 60:
                    try:
                        instances = self.config_manager.get_user_instances(self.username)
                        instance = next((i for i in instances if i["name"] == self.target_name), None)
                        if instance:
                             status = instance.get("status")
                             pod_name = instance.get("podName")
                             print(f"SFTP: Waiting for pod... instance={self.target_name} status={status} podName={pod_name}", file=sys.stderr, flush=True)
                             if status == "Running" and pod_name:
                                break
                        else:
                             print(f"SFTP: Waiting for instance {self.target_name} to appear in list...", file=sys.stderr, flush=True)
                    except Exception as e:
                        print(f"SFTP: Error checking instance status: {e}", file=sys.stderr, flush=True)
                    await asyncio.sleep(2)
                
                if not instance or instance.get("status") != "Running":
                    raise asyncssh.SFTPError(asyncssh.FX_CONNECTION_LOST, "Timed out waiting for pod")
            
            self.pod_name = instance.get("podName")
            print(f"SFTP: Resolved pod_name={self.pod_name}", file=sys.stderr, flush=True)

    async def _exec(self, cmd_args):
        """Execute command in pod via kubectl."""
        await self._ensure_pod()
        # Ensure all args are strings
        str_args = [str(arg) if not isinstance(arg, bytes) else arg.decode('utf-8', errors='replace') for arg in cmd_args]
        
        print(f"SFTP: _exec(cmd={str_args})", file=sys.stderr, flush=True)
        full_cmd = ["kubectl", "exec", self.pod_name, "-n", self.namespace, "--"] + str_args
        proc = await asyncio.create_subprocess_exec(
            *full_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        print(f"SFTP: _exec done (rc={proc.returncode})", file=sys.stderr, flush=True)
        
        if proc.returncode != 0:
            err_msg = stderr.decode(errors='replace')
            if "No such file" in err_msg:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, "File not found")
            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, err_msg)
        
        return stdout.decode(errors='replace').strip()

    async def stat(self, path):
        path = self._norm_path(path)
        print(f"SFTP: stat(path={path})", file=sys.stderr, flush=True)
        try:
            res = await self._exec(["stat", "-c", "%F|%s|%Y|%f", path])
            print(f"SFTP: stat res={res}", file=sys.stderr, flush=True)
            parts = res.split("|")
            if len(parts) != 4:
                raise ValueError(f"Invalid stat output: {res}")
            
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
            if "No such file" in str(e):
                 raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, str(e))
            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))

    async def lstat(self, path):
        return await self.stat(path)

    async def open(self, path, pflags, attrs):
        path = self._norm_path(path)
        print(f"SFTP: open(path={path}, pflags={pflags})", file=sys.stderr, flush=True)
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
                    await self._exec(["sh", "-c", f"cat /dev/null > '{path}'"])
            except Exception as e:
                 print(f"SFTP: warning - failed to truncate/create {path}: {e}", file=sys.stderr, flush=True)
        return WhistlerSFTPFile(self, path, pflags, attrs)

    async def listdir(self, path):
        path = self._norm_path(path)
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
        path = self._norm_path(path)
        await self._exec(["mkdir", "-p", path])
        
    async def rmdir(self, path):
        path = self._norm_path(path)
        await self._exec(["rmdir", path])
        
    async def remove(self, path):
        path = self._norm_path(path)
        await self._exec(["rm", path])
        
    async def rename(self, oldpath, newpath):
        oldpath = self._norm_path(oldpath)
        newpath = self._norm_path(newpath)
        await self._exec(["mv", oldpath, newpath])

