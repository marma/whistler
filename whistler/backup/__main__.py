"""``python -m whistler.backup``: export, inspect and restore state.

Runs where the RBAC already is, in the operator pod, and uses only stdin and
stdout, so nothing is written inside the container::

    kubectl -n whistler exec deploy/whistler-operator -- \\
        python -m whistler.backup export > whistler-backup.tar.gz
    kubectl -n whistler exec -i deploy/whistler-operator -- \\
        python -m whistler.backup restore < whistler-backup.tar.gz
    kubectl -n whistler exec -i deploy/whistler-operator -- \\
        python -m whistler.backup restore --apply < whistler-backup.tar.gz

``restore`` previews unless ``--apply`` is given: with the backup arriving
on stdin there is no way to ask for confirmation, so the default is the one
that writes nothing.

The secrets passphrase is read from the environment
(``WHISTLER_BACKUP_PASSPHRASE``, or the variable ``--passphrase-env``
names), never from an argument, which would put it in the process list.
Through kubectl: ``kubectl exec ... -- env WHISTLER_BACKUP_PASSPHRASE=...``.

This is also the way in when the portal is what is broken.
"""

import argparse
import json
import logging
import os
import sys

from whistler.backup import BackupError, archive

PASSPHRASE_ENV = "WHISTLER_BACKUP_PASSPHRASE"


def _config_manager():
    from whistler.config import KubeConfigManager
    return KubeConfigManager()


def _passphrase(args):
    return os.environ.get(args.passphrase_env) or None


def _say(*lines):
    for line in lines:
        print(line, file=sys.stderr)


def cmd_export(args) -> int:
    from whistler.backup.export import export
    if sys.stdout.isatty():
        _say("Refusing to write a backup to a terminal; redirect it to a file.")
        return 2
    data, manifest, warnings = export(
        _config_manager(), include_secrets=not args.no_secrets,
        passphrase=_passphrase(args), trigger=args.trigger)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
    _say(f"Exported {sum(manifest['counts'].values())} object(s) and "
         f"{manifest['secretCount']} secret(s) ({manifest['secrets']}) "
         f"as {archive.filename(manifest)}", *[f"warning: {w}" for w in warnings])
    return 0


def _read(args) -> archive.Backup:
    return archive.read(sys.stdin.buffer.read(), passphrase=_passphrase(args))


def cmd_inspect(args) -> int:
    backup = _read(args)
    print(json.dumps(backup.manifest, indent=2, sort_keys=True))
    if args.objects:
        for obj in backup.objects:
            meta = obj["metadata"]
            print(f"{obj['kind']:<11} {meta.get('namespace') or '(release)'}/"
                  f"{meta['name']}")
    return 0


def cmd_restore(args) -> int:
    from whistler.backup import restore
    from whistler.backup.export import export
    backup = _read(args)
    cm = _config_manager()
    the_plan = restore.plan(cm, backup, include_secrets=not args.no_secrets)
    for entry in the_plan.entries:
        if entry.action != restore.UNCHANGED or args.verbose:
            reason = f"  ({entry.reason})" if entry.reason else ""
            print(f"{entry.action:<9} {entry.kind:<11} "
                  f"{entry.namespace}/{entry.name}{reason}")
    counts = the_plan.counts()
    print("summary: " + ", ".join(f"{n} {a}" for a, n in sorted(counts.items())))
    for w in the_plan.warnings:
        print(f"warning: {w}")
    if not args.apply:
        print("Nothing written (preview). Run again with --apply to restore.")
        return 0
    if not any(e.action in (restore.CREATE, restore.REPLACE)
               for e in the_plan.entries):
        print("Nothing to restore: the cluster already matches the backup.")
        return 0
    if args.pre_restore:
        data, manifest, _ = export(cm, passphrase=_passphrase(args),
                                   trigger="pre-restore")
        with open(args.pre_restore, "wb") as f:
            f.write(data)
        print(f"Saved the current state to {args.pre_restore} first.")
    written = restore.apply(cm, the_plan)
    print(f"Restored {len(written)} object(s).")
    pruned = restore.readback(cm, written)
    if pruned:
        for line in pruned:
            print(f"pruned: {line}")
        print(restore.PRUNED_HINT)
        return 1
    return 0


def cmd_serve(args) -> int:
    from whistler.backup.service import serve
    serve()
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m whistler.backup",
                                     description=__doc__.split("\n\n")[0])
    parser.add_argument("--passphrase-env", default=PASSPHRASE_ENV,
                        help=f"environment variable holding the secrets "
                             f"passphrase (default {PASSPHRASE_ENV})")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("export", help="write a backup to stdout")
    p.add_argument("--no-secrets", action="store_true",
                   help="leave out the SSH CA, host key and dataset credentials")
    p.add_argument("--trigger", default="manual", help=argparse.SUPPRESS)
    p.set_defaults(fn=cmd_export)

    p = sub.add_parser("inspect", help="print a backup's manifest (stdin)")
    p.add_argument("--objects", action="store_true", help="list its objects")
    p.set_defaults(fn=cmd_inspect)

    p = sub.add_parser("restore", help="restore a backup (stdin); preview "
                                       "unless --apply")
    p.add_argument("--apply", action="store_true", help="write the changes")
    p.add_argument("--no-secrets", action="store_true",
                   help="restore the objects only")
    p.add_argument("--pre-restore", metavar="PATH",
                   help="first save the current state to PATH")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="also list unchanged objects")
    p.set_defaults(fn=cmd_restore)

    p = sub.add_parser("serve", help="run the backup service (the pod that "
                                     "mounts the backup volume)")
    p.set_defaults(fn=cmd_serve)

    args = parser.parse_args(argv)
    level = os.environ.get("WHISTLER_BACKUP_LOG_LEVEL", "WARNING").upper()
    logging.basicConfig(level=level, format="%(name)s: %(message)s",
                        stream=sys.stderr)
    # At DEBUG the kubernetes client logs every response body, Secrets
    # included, and this command reads the CA's private key.
    from whistler.logsetup import quiet_chatty_libraries
    quiet_chatty_libraries(level)
    try:
        return args.fn(args)
    except BackupError as e:
        _say(f"error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
