import asyncssh
import asyncio
import sys
import time
import stat
import logging
from whistler.globals import CONN_SERVER_MAP as _CONN_SERVER_MAP

logger = logging.getLogger(__name__)

class WhistlerSFTPFile:
    """SFTP File Handle that streams via kubectl exec."""
    def __init__(self, sftp_server, path, flags, attrs):
        logger.debug(f"SFTP: WhistlerSFTPFile created for {path} (flags={flags}, attrs={attrs})")
        object.__setattr__(self, 'sftp_server', sftp_server)
        object.__setattr__(self, 'path', sftp_server._norm_path(path))
        object.__setattr__(self, 'flags', flags)
        object.__setattr__(self, 'attrs', attrs)
        object.__setattr__(self, 'pos', 0)
        object.__setattr__(self, 'proc', None)
        object.__setattr__(self, '_stream_pos', 0)
        object.__setattr__(self, '_closing', False)
        object.__setattr__(self, '_write_lock', asyncio.Lock())

    def fileno(self):
        return None

    async def writev(self, chunks, offset):
        # Fallback to write (inefficient but safe)
        length = 0
        for data in chunks:
            await self._write_actual(data, offset + length)
            length += len(data)
        return length

    def write(self, data):
        """Synchronous wrapper that returns a coroutine"""
        return self._write_actual(data, self.pos)

    async def _write_actual(self, data, offset):
        # logging at top to verify call
        # print(f"SFTP: _write_actual(len={len(data)}, offset={offset})", file=sys.stderr, flush=True)

        if self._closing:
            raise asyncssh.SFTPError(asyncssh.FX_BAD_MESSAGE, "File is closed")

        async with self._write_lock:
            # Retry loop (max 1 retry)
            for attempt in range(2):
                try:
                    await self.sftp_server._ensure_pod()
                    if not self.sftp_server.pod_name:
                        raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No active pod found")

                    # Check if we can reuse the existing process
                    # We can reuse if the process exists AND the requested write offset matches 
                    # exactly where the process stream is currently positioned.
                    if self.proc:
                        if offset != self._stream_pos:
                            logger.debug(f"SFTP: offset mismatch (req={offset}, stream={self._stream_pos}), restarting writer")
                            await self._stop_process()
                    
                    if not self.proc:
                        # Use truncate to ensure we start writing at the exact correct offset
                        if offset == 0:
                            shell_cmd = f"(cat > '{self.path}')"
                        else:
                            shell_cmd = f"(truncate -s {offset} '{self.path}' && cat >> '{self.path}')"

                        cmd = ["kubectl", "exec", "-i", self.sftp_server.pod_name,
                               "-n", self.sftp_server.namespace, "--",
                               "sh", "-c", shell_cmd]

                        try:
                            logger.debug(f"SFTP: Starting upload process (attempt {attempt+1}): {cmd}")
                            self.proc = await asyncio.create_subprocess_exec(
                                *cmd, stdin=asyncio.subprocess.PIPE,
                                stdout=asyncio.subprocess.DEVNULL,
                                stderr=asyncio.subprocess.PIPE)

                            # Initialize stream position to the starting offset
                            self._stream_pos = offset

                            async def consume_stderr(proc, path):
                                try:
                                    while True:
                                        line = await proc.stderr.readline()
                                        if not line: break
                                        logger.warning(f"SFTP Write Stderr ({path}): {line.decode().strip()}")
                                except Exception:
                                    pass
                            asyncio.create_task(consume_stderr(self.proc, self.path))

                            # Give a tiny bit of time for immediate failures (like 'No such file')
                            await asyncio.sleep(0.1)
                            if self.proc.returncode is not None:
                                raise Exception(f"Process exited immediately with code {self.proc.returncode}")

                        except Exception as e:
                            if attempt == 0:
                                await self._stop_process()
                                continue
                            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))
                        
                        # Sync file pos to offset as well if we just started
                        self.pos = offset

                    self.proc.stdin.write(data)
                    await self.proc.stdin.drain()
                    
                    # Update both positions
                    self.pos += len(data)
                    self._stream_pos += len(data)
                    break

                except (BrokenPipeError, ConnectionResetError) as e:
                    logger.debug(f"SFTP: write broken pipe (attempt {attempt+1}): {e}")
                    await self._stop_process()
                    if attempt == 1:
                        raise asyncssh.SFTPError(asyncssh.FX_FAILURE, f"Write failed after retry: {e}")
                except Exception as e:
                    logger.error(f"SFTP: write failed for {self.path} (attempt {attempt+1}): {e}")
                    if attempt == 1:
                        raise asyncssh.SFTPError(asyncssh.FX_FAILURE, str(e))

        return len(data)

    def seek(self, offset, whence=0):
        logger.debug(f"SFTP: seek({offset}, whence={whence})")
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
        logger.debug(f"SFTP: read(size={size}, offset={offset})")
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

    def readable(self):
        return True

    def writable(self):
        return True

    def seekable(self):
        return True

    def chmod(self, mode):
        logger.debug(f"SFTP: chmod({mode}) on {self.path}")
        return None

    def chown(self, uid, gid):
        logger.debug(f"SFTP: chown({uid}, {gid}) on {self.path}")
        return None

    def chgrp(self, gid):
        logger.debug(f"SFTP: chgrp({gid}) on {self.path}")
        return None

    def utime(self, times):
        logger.debug(f"SFTP: utime({times}) on {self.path}")
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
        """Mark file as closed and clean up resources."""
        logger.debug(f"SFTP: close({self.path})")
        self._closing = True
        asyncio.create_task(self._stop_process())

    async def close_async(self):
        """Asynchronous cleanup."""
        await self._stop_process()

    async def stat(self):
        return await self.sftp_server.stat(self.path)
        
    def setstat(self, attrs):
        logger.debug(f"SFTP: setstat({self.path}, {attrs}) - ignoring")
        # Explicitly ignore setstat to satisfy scp -p
        # Just return None for success
        return None

    def truncate(self, size):
        logger.debug(f"SFTP: truncate({size}) on {self.path} - ignoring/handled by write")
        # We can implement this if needed, but for now just acknowledge.
        # If we really need validation, we can launch async task?
        # For scp upload, it's advisory.
        return size

class WhistlerSFTPServer(asyncssh.SFTPServer):
    """SFTP Server that translates operations to kubectl exec commands."""
    def __init__(self, chan):
        logger.debug(f"WhistlerSFTPServer initializing with channel={chan}")
        super().__init__(chan)

        # Retrieve session info stored on the channel
        session = getattr(chan, '_whistler_session', None)
        if not session:
            raise ValueError("Channel does not have _whistler_session attribute")

        self._session = session
        self._chan = chan
        self.username = session.username
        self.target_type = session.target_type
        # target_name might be a template initially, _ensure_pod will resolve it
        self.target_name = session.target_name
        self.config_manager = session.config_manager
        self.pod_name = None
        self.namespace = self.config_manager.namespace
        self._lock = asyncio.Lock()
        # Cache for recently closed files to handle late fsetstat operations
        # self._closed_files = {}  # path -> file object - REMOVED
        logger.debug(f"SFTP context: user={self.username}, target={self.target_name}, type={self.target_type}")
        
    def _norm_path(self, path):
        if isinstance(path, bytes):
            return path.decode('utf-8', errors='replace')
        return path

    async def _ensure_pod(self):
        """Ensure target pod is running, start if necessary."""
        async with self._lock:
            if self.pod_name:
                return
                
            logger.debug(f"SFTP: _ensure_pod(user={self.username}, target={self.target_name}, type={self.target_type})")
            if not self.username:
                raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, "No username found in session")

            loop = asyncio.get_running_loop()

            # Check for existing instance first
            try:
                instances = await loop.run_in_executor(None, self.config_manager.get_user_instances, self.username)
            except Exception as e:
                logger.error(f"SFTP Error: Failed to get user instances: {e}")
                raise asyncssh.SFTPError(asyncssh.FX_FAILURE, f"Failed to list instances: {e}")

            instance = next((i for i in instances if i["name"] == self.target_name), None)
            
            if not instance and self.target_type == "template":
                # Use the template_name resolved in WhistlerSession
                template_name = getattr(self._session, 'template_name', None) or self.target_name
                logger.debug(f"SFTP: Instance {self.target_name} not found, checking template {template_name}")
                
                templates = await loop.run_in_executor(None, self.config_manager.get_user_templates, self.username)
                template_obj = next((t for t in templates if t["name"] == template_name), None)
                
                if template_obj:
                    template_ref = template_obj.get("fullName", template_name)
                    logger.info(f"SFTP: Creating anonymous instance {self.target_name} from template {template_ref}")
                    
                    success = await loop.run_in_executor(None, lambda: self.config_manager.add_instance(self.username, template_ref, self.target_name, preemptible=True))
                    if not success:
                        raise asyncssh.SFTPError(asyncssh.FX_FAILURE, "Failed to create instance from template")
                    
                    # Wait for instance to appear
                    start_time = time.time()
                    while time.time() - start_time < 30:
                        instances = await loop.run_in_executor(None, self.config_manager.get_user_instances, self.username)
                        instance = next((i for i in instances if i["name"] == self.target_name), None)
                        if instance:
                             break
                        await asyncio.sleep(1)
                
                if not instance:
                    logger.error(f"SFTP Error: Could not resolve target {self.target_name} to instance or template")
                    raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, f"Target {self.target_name} not found")

            if not instance:
                # If target_type was "instance" but not found
                logger.error(f"SFTP Error: Instance {self.target_name} not found")
                raise asyncssh.SFTPError(asyncssh.FX_NO_CONNECTION, f"Instance {self.target_name} not found")
            
            self.namespace = instance.get("namespace", f"whistler-user-{self.username}")
            logger.debug(f"SFTP: Target instance {self.target_name} status={instance.get('status')} in ns={self.namespace}")
            
            # Start instance if not running
            if instance.get("status") != "Running" or not instance.get("podName"):
                try:
                    full_cr_name = f"{self.username}-{self.target_name}"
                    logger.info(f"SFTP: Patching instance {full_cr_name} to start (namespace={self.namespace})...")
                    
                    def patch_instance():
                        self.config_manager.api.patch_namespaced_custom_object(
                            self.config_manager.group, self.config_manager.version, self.namespace,
                            "sessions", full_cr_name,
                            {"metadata": {"annotations": {"whistler/last-connect": str(time.time())}}})
                    
                    await loop.run_in_executor(None, patch_instance)
                except Exception as e:
                    logger.error(f"SFTP Error: Failed to start instance {full_cr_name}: {e}")
                    raise asyncssh.SFTPError(asyncssh.FX_FAILURE, f"Failed to start instance: {e}")
                
                # Wait for running (60s timeout)
                start_time = time.time()
                while time.time() - start_time < 60:
                    try:
                        instances = await loop.run_in_executor(None, self.config_manager.get_user_instances, self.username)
                        instance = next((i for i in instances if i["name"] == self.target_name), None)
                        if instance:
                             status = instance.get("status")
                             pod_name = instance.get("podName")
                             logger.debug(f"SFTP: Waiting for pod... instance={self.target_name} status={status} podName={pod_name}")
                             if status == "Running" and pod_name:
                                break
                        else:
                             logger.debug(f"SFTP: Waiting for instance {self.target_name} to appear in list...")
                    except Exception as e:
                        logger.error(f"SFTP: Error checking instance status: {e}")
                    await asyncio.sleep(2)
                
                if not instance or instance.get("status") != "Running":
                    raise asyncssh.SFTPError(asyncssh.FX_CONNECTION_LOST, "Timed out waiting for pod")
            
            self.pod_name = instance.get("podName")
            logger.debug(f"SFTP: Resolved pod_name={self.pod_name}")

    async def _exec(self, cmd_args):
        """Execute command in pod via kubectl."""
        await self._ensure_pod()
        # Ensure all args are strings
        str_args = [str(arg) if not isinstance(arg, bytes) else arg.decode('utf-8', errors='replace') for arg in cmd_args]
        
        logger.debug(f"SFTP: _exec(cmd={str_args})")
        full_cmd = ["kubectl", "exec", self.pod_name, "-n", self.namespace, "--"] + str_args
        proc = await asyncio.create_subprocess_exec(
            *full_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        logger.debug(f"SFTP: _exec done (rc={proc.returncode})")
        
        if proc.returncode != 0:
            err_msg = stderr.decode(errors='replace')
            if "No such file" in err_msg:
                raise asyncssh.SFTPError(asyncssh.FX_NO_SUCH_FILE, "File not found")
            raise asyncssh.SFTPError(asyncssh.FX_FAILURE, err_msg)
        
        return stdout.decode(errors='replace').strip()

    async def stat(self, path):
        path = self._norm_path(path)
        logger.debug(f"SFTP: stat(path={path})")
        try:
            res = await self._exec(["stat", "-c", "%F|%s|%Y|%f", path])
            logger.debug(f"SFTP: stat res={res}")
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
        logger.debug(f"SFTP: open(path={path}, pflags={pflags})")
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
                    logger.debug(f"SFTP: creating/truncating {path}")
                    await self._exec(["sh", "-c", f"cat /dev/null > '{path}'"])
            except Exception as e:
                 logger.warning(f"SFTP: warning - failed to truncate/create {path}: {e}")

        f = WhistlerSFTPFile(self, path, pflags, attrs)
        logger.debug(f"SFTP: open returning file object {f}")
        return f

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

    async def fstat(self, file_obj):
        logger.debug(f"SFTP: fstat(file_obj={file_obj})")
        attrs = asyncssh.SFTPAttrs()
        attrs.size = getattr(file_obj, 'pos', 0)
        attrs.permissions = stat.S_IFREG | 0o644
        return attrs

    async def fsetstat(self, file_obj, attrs):
        logger.debug(f"SFTP: fsetstat(file_obj={file_obj}, attrs={attrs})")
        if hasattr(file_obj, 'setstat'):
            file_obj.setstat(attrs)
        return None

    async def setstat(self, path, attrs):
        path = self._norm_path(path)
        logger.debug(f"SFTP: setstat on path {path}, attrs={attrs} - ignoring")
        return

