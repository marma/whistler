# Whistler Security Audit Report

## Executive Summary

I've completed a comprehensive security audit of the Whistler project - a Kubernetes operator that provides SSH access to ephemeral and persistent pods. The audit identified **multiple critical and high-severity vulnerabilities** that require immediate attention. The most significant issues involve command injection vulnerabilities, insecure authentication bypass options, and overly permissive RBAC configurations.

**Overall Risk Level: HIGH**

---

## Critical Findings

### 1. Command Injection Vulnerabilities (CRITICAL)

**Severity: CRITICAL | CVSS: 9.8**

Multiple locations allow shell command injection through unsanitized user input:

#### 1.1 SFTP Path Injection
**Location:** `whistler/sftp_support.py:67-72`
```python
shell_cmd = f"(truncate -s {offset} '{self.path}' && cat >> '{self.path}')"
```
- File paths from SFTP clients are embedded directly into shell commands
- Single quotes can be escaped with `'\'` allowing arbitrary command execution
- **Attack Vector:** Upload file with path like `foo' && malicious_cmd #`

#### 1.2 SFTP File Creation
**Location:** `whistler/sftp_support.py:452`
```python
await self._exec(["sh", "-c", f"cat /dev/null > '{path}'"])
```
- Same vulnerability as above in file creation

#### 1.3 Port Forwarding Command Injection
**Location:** `whistler/server.py:407-432`
```python
cmd_str = f"socat - TCP4:127.0.0.1:{port}"
```
- Port parameter is user-controlled through SSH forwarding requests
- No validation that `port` is a number
- Could inject commands through malicious port specifications

#### 1.4 Static Binary Injection
**Location:** `whistler/server.py:498`
```python
"sh", "-c", f"cat > {target_path} && chmod +x {target_path}"
```
- `target_path` could be manipulated if socat binary path is controlled

**Recommendation:** Use parameterized commands or proper escaping. Switch to direct `kubectl cp` or Python kubernetes client exec API instead of shell commands.

---

### 2. Authentication Bypass (CRITICAL)

**Severity: CRITICAL | CVSS: 9.1**

**Location:** `whistler/server.py:260-285, 296-313`

The `WHISTLER_AUTH_ALLOW_ANY` environment variable completely bypasses authentication:

```python
def password_auth_supported(self):
    return os.environ.get("WHISTLER_AUTH_ALLOW_ANY") == "true"

def validate_password(self, username, password):
    if os.environ.get("WHISTLER_AUTH_ALLOW_ANY") != "true":
        return False
    # ... accepts ANY password for ANY username
    return True
```

**Issues:**
- Accepts any username/password combination
- Accepts any SSH key without validation
- Intended for development but could be accidentally enabled in production
- No warnings or safeguards against production use

**Recommendation:**
- Remove this feature entirely or require additional safeguards
- Add explicit warnings in logs when enabled
- Prevent enabling in production environments via Helm validation

---

### 3. Weak Public Key Authentication (HIGH)

**Severity: HIGH | CVSS: 7.5**

**Location:** `whistler/server.py:321-327`

```python
key_data = key.export_public_key().decode('utf-8').split()[1]
for allowed in allowed_keys:
    if key_data in allowed:  # Substring match!
        return True
```

**Issues:**
- Uses substring matching instead of exact key comparison
- A short collision in base64 data could authenticate wrong users
- No timing-safe comparison

**Recommendation:** Use exact key matching with constant-time comparison.

---

### 4. Overly Permissive RBAC (HIGH)

**Severity: HIGH | CVSS: 7.8**

**Location:** `charts/whistler/templates/rbac.yaml`

The operator has excessive cluster-wide permissions:

```yaml
ClusterRole rules:
- resources: [namespaces]
  verbs: [create, delete, update, patch]  # Can delete ANY namespace!

- resources: [secrets, configmaps]
  verbs: [create, delete, get, list, patch, update, watch]  # Access to ALL secrets!

- resources: [pods/exec]
  verbs: [create, delete, get, list, patch, update, watch]  # Exec into ANY pod!
```

**Issues:**
- Can delete any namespace in the cluster
- Can read all secrets cluster-wide (including credentials, API keys, etc.)
- Can execute commands in any pod via `pods/exec`
- No namespace restriction despite per-user namespaces
- Server component likely has same permissions

**Recommendation:**
- Restrict to specific namespaces (whistler-user-* pattern)
- Use RoleBindings per namespace instead of ClusterRoleBinding
- Remove secrets access if not needed
- Audit actual permission requirements

---

## High Severity Findings

### 5. Container Running as Root (HIGH)

**Severity: HIGH | CVSS: 7.2**

**Location:** `Dockerfile` (no USER directive)

```dockerfile
FROM python:3.13-slim
# ... no USER directive
CMD ["python", "-m", "whistler.server"]
```

**Issues:**
- Whistler server and operator run as root (UID 0)
- User pods created with no security context (`config.py:606-640`)
- Increased impact of container escape vulnerabilities
- Violates principle of least privilege

**Recommendation:**
- Add `USER 1000` directive to Dockerfile
- Set pod security contexts with `runAsNonRoot: true`
- Add `seccompProfile`, `allowPrivilegeEscalation: false`

---

### 6. User Pod Privilege Escalation Risk (HIGH)

**Severity: HIGH | CVSS: 7.4**

**Location:** `whistler/config.py:606-640`

User pods are created with no security restrictions:

```python
pod_body = {
    "spec": {
        "containers": [{
            "name": "main",
            "image": image,  # User-controlled image!
            "command": ["sleep", "3600"],
            # NO securityContext!
        }]
    }
}
```

**Issues:**
- No security context enforcement
- Users can potentially escalate privileges within their pods
- No AppArmor or SELinux profiles
- No capabilities restrictions
- Images are user-controlled via templates

**Recommendation:**
```python
"securityContext": {
    "runAsNonRoot": true,
    "allowPrivilegeEscalation": false,
    "capabilities": {"drop": ["ALL"]},
    "seccompProfile": {"type": "RuntimeDefault"}
}
```

---

### 7. Insufficient Input Validation (HIGH)

**Severity: HIGH | CVSS: 6.8**

Multiple areas lack input validation:

#### 7.1 Instance/Template Name Validation
**Location:** `whistler/server.py:269-284`
```python
parts = username.split('-')
real_user = parts[0]
suffix = "-".join(parts[1:])  # No validation!
```

- No validation of username format
- No sanitization of instance/template names
- Could contain path traversal characters
- Could cause namespace naming issues

#### 7.2 SFTP Path Traversal
**Location:** `whistler/sftp_support.py:282-285`
```python
def _norm_path(self, path):
    if isinstance(path, bytes):
        return path.decode('utf-8', errors='replace')
    return path  # No validation!
```

- No validation against path traversal (`../../../etc/passwd`)
- Could access files outside intended directory
- No check for absolute paths vs relative paths

**Recommendation:**
- Validate usernames against `^[a-z0-9-]+$`
- Resolve and validate paths against allowed directories
- Reject paths containing `..` or starting with `/`

---

### 8. Secrets in Version Control Risk (MEDIUM-HIGH)

**Severity: MEDIUM-HIGH | CVSS: 6.5**

**Location:** `ssh_host_key` (referenced in `server.py:220`)

```python
await asyncssh.create_server(server_factory, '', 8022,
                             server_host_keys=['ssh_host_key'],  # Filesystem path
```

**Issues:**
- SSH host key stored in filesystem
- Potentially committed to git (found in helm chart references)
- Same key used across all deployments
- No key rotation mechanism
- Enables MITM attacks if key is compromised

**Recommendation:**
- Generate host keys at deployment time
- Store in Kubernetes Secrets
- Implement key rotation
- Add `ssh_host_key*` to `.gitignore`

---

## Medium Severity Findings

### 9. No Rate Limiting on Authentication (MEDIUM)

**Severity: MEDIUM | CVSS: 5.9**

**Location:** `whistler/server.py:251-347`

No rate limiting or account lockout mechanism for failed authentication attempts.

**Issues:**
- Enables brute force attacks on SSH keys
- No exponential backoff
- No IP-based blocking
- No audit logging of failed attempts

**Recommendation:**
- Implement rate limiting per IP/username
- Add connection throttling
- Log all authentication attempts
- Consider fail2ban integration

---

### 10. Information Disclosure via Error Messages (MEDIUM)

**Severity: MEDIUM | CVSS: 5.3**

**Location:** Multiple locations with verbose error logging

```python
print(f"Failed to create tunnel: {e}", file=sys.stderr)
logger.error(f"Failed to list templates in {ns}: {e}")
```

**Issues:**
- Stack traces exposed to SSH clients
- Internal paths and configuration revealed
- Pod names and namespaces leaked
- Kubernetes API errors exposed

**Recommendation:**
- Return generic error messages to clients
- Log detailed errors server-side only
- Sanitize error messages before display

---

### 11. No TLS for Internal Communication (MEDIUM)

**Severity: MEDIUM | CVSS: 5.0**

**Location:** All kubectl exec operations

All communication with pods via `kubectl exec` uses unencrypted stdin/stdout streams after the initial SSH connection terminates.

**Issues:**
- Traffic between whistler server and Kubernetes API is encrypted by kubectl
- But data flowing through pods is not end-to-end encrypted within cluster
- Network policies only deny ingress, not egress

**Recommendation:**
- Document that cluster network security is required
- Consider additional encryption layer for sensitive data
- Ensure Kubernetes network policies are properly configured

---

### 12. Pod Command Injection Risk (MEDIUM)

**Severity: MEDIUM | CVSS: 5.8**

**Location:** `whistler/server.py:1226-1239`

```python
b64_cmd = base64.b64encode(command.encode('utf-8')).decode('utf-8')
script_path = f"/tmp/whistler-exec-{secrets.token_hex(4)}.sh"
wrapped_cmd = f"echo {b64_cmd} | base64 -d > {script_path} && sh -l {script_path}; rm -f {script_path}"
cmd.extend(["bash", "-c", wrapped_cmd])
```

**Issues:**
- While base64 encoded, the script path and shell execution could be manipulated
- Uses `sh -l` which executes profile scripts (could be malicious)
- Temporary file in `/tmp` with predictable pattern
- Race condition between write and execute

**Recommendation:**
- Use stdin redirection: `bash -c "$(echo $B64 | base64 -d)"`
- Or use kubectl exec without intermediate shell

---

### 13. Dependency Versions (LOW-MEDIUM)

**Severity: LOW-MEDIUM | CVSS: 4.0**

**Location:** `pyproject.toml`

```toml
dependencies = [
    "textual>=6.6.0",
    "asyncssh>=2.14.0",
    "pyyaml>=6.0.1",
    "kopf>=1.37.0",
    "kubernetes>=29.0.0",
]
```

**Issues:**
- Minimum version specifiers allow potentially vulnerable future versions
- No upper bounds or pinning
- PyYAML has history of CVEs
- No automated dependency scanning visible

**Recommendation:**
- Pin exact versions or use `~=` for compatible releases
- Implement Dependabot or Renovate
- Regular security scanning with tools like `pip-audit` or `safety`

---

### 14. Network Policy Gaps (MEDIUM)

**Severity: MEDIUM | CVSS: 5.4**

**Location:** `whistler/config.py:124-136`

```python
"spec": {
    "podSelector": {},
    "policyTypes": ["Ingress"],
    "ingress": []  # Deny all ingress
}
```

**Issues:**
- Only restricts ingress, not egress
- Users can make outbound connections to any service
- Could be used for data exfiltration
- No DNS restrictions
- Pods can communicate with each other within namespace

**Recommendation:**
- Add egress rules limiting outbound connections
- Restrict DNS access
- Block pod-to-pod communication if not needed
- Consider service mesh for finer-grained control

---

## Low Severity / Informational Findings

### 15. Debug Logging Enabled (LOW)

**Location:** `whistler/server.py:205-212`

Logging level configurable via environment variable, defaults to DEBUG:
```python
log_level = os.environ.get("WHISTLER_LOG_LEVEL", "DEBUG").upper()
```

- May log sensitive information in production
- Recommendation: Default to INFO or WARNING

### 16. No Session Timeout (LOW)

SSH sessions have keepalive but no maximum duration enforcement.

- Recommendation: Implement session timeout for ephemeral pods

### 17. Missing Resource Quotas (LOW)

**Location:** `whistler/config.py`

No namespace resource quotas configured.

- Users could consume excessive cluster resources
- Recommendation: Add ResourceQuota objects to user namespaces

### 18. No Audit Logging (LOW)

No centralized audit log for user actions:
- File access via SFTP
- Commands executed
- Port forwarding sessions

- Recommendation: Implement audit logging to external system

---

## Recommendations Summary

### Immediate Actions (Critical/High)

1. **Fix command injection vulnerabilities** - Use parameterized commands or proper escaping
2. **Remove or secure authentication bypass** - Add production safeguards
3. **Restrict RBAC permissions** - Scope to specific namespaces only
4. **Add security contexts** - Run as non-root with dropped capabilities
5. **Implement input validation** - Sanitize all user inputs
6. **Rotate SSH host keys** - Move to Kubernetes Secrets

### Short-term Actions (Medium)

7. **Add rate limiting** - Prevent brute force attacks
8. **Sanitize error messages** - Prevent information disclosure
9. **Add egress network policies** - Restrict outbound connections
10. **Pin dependency versions** - Enable security scanning
11. **Add audit logging** - Track user actions

### Long-term Improvements (Low)

12. **Implement session timeouts**
13. **Add resource quotas**
14. **Enable security scanning** in CI/CD
15. **Consider security hardening guides** for Kubernetes

---

## Conclusion

Whistler is an innovative tool for providing on-demand SSH access to Kubernetes pods. However, the current implementation has significant security vulnerabilities that must be addressed before production deployment. The command injection vulnerabilities and overly permissive RBAC are the most critical issues requiring immediate remediation.

The project would benefit from:
- Security-focused code review
- Penetration testing
- Implementation of secure coding practices
- Regular security audits
- Automated security scanning in CI/CD

With proper security hardening, Whistler could be a secure and valuable tool for Kubernetes development workflows.

---

**Audit Date:** 2026-01-09
**Auditor:** Claude (Anthropic)
**Project Version:** 0.1.0
