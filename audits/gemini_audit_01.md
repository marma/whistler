# Security Audit: Whistler Project

**Date:** 2026-01-09
**Auditor:** Gemini

## 1. Executive Summary

This audit reviewed the Whistler project codebase, focusing on the SSH server (`whistler/server.py`), configuration management (`whistler/config.py`), and Kubernetes deployment charts (`charts/whistler`).

**Key Finding:** The SSH server runs with an overly permissive Service Account (ClusterRole) intended for the Operator, granting it full control over the Kubernetes cluster. This is a critical security risk. If the SSH server is compromised, the entire cluster is compromised.

## 2. Architecture Review

The system consists of:
*   **Whistler Operator**: Manages `WhistlerInstance` and `WhistlerTemplate` CRDs.
*   **Whistler SSH Server**: Accepts SSH connections, authenticates users, and tunnels connections to User Pods.
*   **User Pods**: Per-user environments created on-demand.

The SSH Server is the primary entry point and attack surface. It authenticates users via Public Key (or password in dev mode) and uses `kubectl exec` to bridge traffic to pods.

## 3. Vulnerability Findings

### 3.1. Excessive Privileges (Critical)
*   **Description:** The SSH Server deployment (`server-deployment.yaml`) uses the same ServiceAccount as the Operator (`{{ .Release.Name }}-operator`).
*   **Evidence:** `charts/whistler/templates/server-deployment.yaml` sets `serviceAccountName` to the operator's SA. `charts/whistler/templates/rbac.yaml` grants this SA `ClusterRole` permissions including `*` on `namespaces`, `networkpolicies`, `pods`, `secrets`, etc.
*   **Impact:** A successful exploit of the SSH server (e.g., via a vulnerability in `asyncssh` or Python code) would give the attacker Cluster Admin privileges.
*   **Recommendation:** Create a dedicated ServiceAccount for the SSH Server with minimal permissions:
    *   `get`, `list`, `watch` on `WhistlerInstance`, `WhistlerTemplate`.
    *   `create`, `get`, `list` on `Pods/exec` (specifically for tunneling).
    *   `get` on `Secrets` (for user public keys, if stored there).
    *   It should **not** have permissions to manage Namespaces, NetworkPolicies, or other system resources.

### 3.2. Authentication Bypass / Dev Mode Risk (High)
*   **Description:** The environment variable `WHISTLER_AUTH_ALLOW_ANY` enables a "dev mode" that unknowingly allows any password and bypasses public key checks.
*   **Evidence:** `whistler/server.py`: `if os.environ.get("WHISTLER_AUTH_ALLOW_ANY") == "true":` -> bypasses validation.
*   **Impact:** If accidentally enabled in production (e.g., via misconfigured Helm values), anyone can log in as any user.
*   **Recommendation:** Ensure this variable is strictly controlled. Consider removing this logic from production builds or logging a FATAL warning if enabled. In `charts/whistler/values.yaml`, default this to `false` (it is essentially unconfigured currently, but explicitly setting it to false is safer).

### 3.3. Pod Security Context (Medium)
*   **Description:** User pods created by `ensure_pod` in `config.py` do not define a `securityContext`.
*   **Evidence:** `whistler/config.py`: Pod spec creation does not include `securityContext`.
*   **Impact:** Containers likely run as `root` (UID 0). While `kubectl exec` access is restricted to the pod, running as root increases the blast radius if container escape vulnerabilities exist.
*   **Recommendation:** Define a default `securityContext` in `WhistlerTemplate` or enforce a `restricted` Pod Security Standard. Ideally, run user workloads as non-root users.

### 3.4. Lack of Resource Limits Enforcement (Medium)
*   **Description:** While `WhistlerTemplate` supports resources, there is no enforcement or quota management at the namespace level (ResourceQuotas). User pods could potentially consume excessive cluster resources.
*   **Impact:** Denial of Service (DoS) for other tenants or system components.
*   **Recommendation:** Deploy `ResourceQuota` objects alongside `NetworkPolicy` in the `_ensure_user_namespace` method.

### 3.5. Command Injection Prevention (Good)
*   **Observation:** The code uses `subprocess.create_subprocess_exec` with list arguments effectively preventing shell injection in most places.
*   **Detail:** User commands are Base64 encoded before being passed to `bash -c`, avoiding direct shell interpretation of user input.
    *   `whistler/server.py`: `f"echo {b64_cmd} | base64 -d > {script_path} ..."`
*   **Status:** Secure as implemented.

### 3.6. Network Isolation (Good)
*   **Observation:** `NetworkPolicy` with default deny-ingress is created for each user namespace.
*   **Detail:** `whistler/config.py`: `_ensure_user_namespace` creates a `NetworkPolicy` blocking all ingress.
*   **Status:** Good baseline, but consider if Egress filtering is also needed to prevent data exfiltration or scanning of internal services (Metadata service, etc.).

## 4. Recommendations Summary

1.  **Split RBAC**: Immediately separate the SSH Server ServiceAccount from the Operator ServiceAccount. Grant the Server SA only the permissions needed to read configuration and `exec` into pods.
2.  **Harden Auth**: Add a check in `server.py` to refuse startup if `WHISTLER_AUTH_ALLOW_ANY` is active but `IN_CLUSTER` or a "production" flag is set.
3.  **Pod Hardening**: Update `config.py` to apply a default non-root `securityContext` to user pods.
4.  **Egress Control**: Update `isolate-user-pods` NetworkPolicy to restrict Egress access (e.g., block cloud metadata IPs).

