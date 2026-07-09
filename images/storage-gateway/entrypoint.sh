#!/bin/sh
# Provision the per-user identity and run smbd in the foreground.
#
# The user is created with their REAL uid so that `force user` in
# smb.conf makes every file on the PVC land under that uid — pod sessions
# mounting the same PVC then see correctly-owned files.
set -eu

: "${SMB_USER:?SMB_USER is required}"
: "${SMB_UID:?SMB_UID is required}"
PASSWORD_FILE="${SMB_PASSWORD_FILE:-/etc/whistler-smb/password}"

groupadd -g "$SMB_UID" "$SMB_USER"
# No home, no shell: this account exists only as an identity for smbd
# (SMB auth is against the passdb, not /etc/shadow).
useradd -u "$SMB_UID" -g "$SMB_UID" -M -d /nonexistent -s /usr/sbin/nologin "$SMB_USER"

pass="$(cat "$PASSWORD_FILE")"
printf '%s\n%s\n' "$pass" "$pass" | smbpasswd -s -a "$SMB_USER"

# A fresh PVC's root is owned by root (or the provisioner) — writes forced to
# the user's uid would be denied. Claim the share root (non-recursive: on a
# populated home this is the ordinary "home dir owned by its user" no-op).
chown "$SMB_UID:$SMB_UID" /shares/home

sed "s/@USER@/$SMB_USER/g" /etc/samba/smb.conf.template > /etc/samba/smb.conf

exec smbd --foreground --no-process-group --debug-stdout
