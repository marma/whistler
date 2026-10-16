#!/usr/bin/env bash
# Fetch an Ubuntu cloud image into the bake cache, verified, and print the
# cached file's NAME (relative to the cache dir) on stdout. Shared by the VM
# image bakes (desktops/vm-gnome-selkies, images/devbase), whose boot.sh backs
# the bake disk with /cache/<that name>.
#
#   scripts/fetch_cloud_image.sh <url> <cache-dir>
#
# The cache is keyed by CONTENT, not by URL: cloud-images.ubuntu.com publishes
# `releases/<ver>/release/` as a moving pointer to the newest build and
# `releases/<ver>/release-<serial>/` for each build, all with the same file
# name. Keying on the name (as the bakes once did) meant the first download was
# reused forever — `release` never advanced, and a pinned serial silently got
# whatever build happened to be cached. Here, the directory's SHA256SUMS says
# what the file should be; a cached file with that hash is reused, anything
# else is downloaded, checked, and only then moved into place.
#
# A URL whose directory has no SHA256SUMS (a local mirror, a file:// path) is
# cached per URL instead, downloaded once, and reported as unverified.
set -euo pipefail

url="$1"
cache="$2"
dir="${url%/*}"
name="${url##*/}"
mkdir -p "$cache"

log() { echo "$@" >&2; }

# Which build this is, when the directory says (Ubuntu's build-info.txt).
serial=$(curl -fsSL "$dir/unpacked/build-info.txt" 2>/dev/null \
         | sed -n 's/^serial=//p') || true

if sums=$(curl -fsSL "$dir/SHA256SUMS" 2>/dev/null); then
  want=$(awk -v n="$name" '{f=$2; sub(/^\*/, "", f)} f == n {print $1}' <<<"$sums")
  [ -n "$want" ] || { log "ERROR: $name is not listed in $dir/SHA256SUMS"; exit 1; }
  cached="${name%.img}-${want:0:16}.img"
  if [ -s "$cache/$cached" ]; then
    log "base image: $name ${serial:+serial $serial, }sha256 ${want:0:16} (cached)"
  else
    log "base image: downloading $name ${serial:+serial $serial }from $dir"
    curl -fL --progress-bar -o "$cache/$cached.part" "$url"
    got=$(sha256sum "$cache/$cached.part" | cut -d' ' -f1)
    if [ "$got" != "$want" ]; then
      rm -f "$cache/$cached.part"
      log "ERROR: sha256 mismatch for $url (got $got, SHA256SUMS says $want)"
      exit 1
    fi
    mv "$cache/$cached.part" "$cache/$cached"
    log "base image: $name ${serial:+serial $serial, }sha256 ${want:0:16} (verified)"
  fi
else
  cached="${name%.img}-url-$(sha256sum <<<"$url" | cut -c1-16).img"
  if [ ! -s "$cache/$cached" ]; then
    log "base image: downloading $url (no SHA256SUMS beside it: NOT verified)"
    curl -fL --progress-bar -o "$cache/$cached.part" "$url"
    mv "$cache/$cached.part" "$cache/$cached"
  else
    log "base image: $url (cached, unverified)"
  fi
fi

echo "$cached"
