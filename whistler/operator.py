import os
import logging
import threading
import time

import kopf
from kubernetes import client

from whistler.logsetup import quiet_chatty_libraries
from whistler.config import (
    ARCHIVE_ANNOTATION,
    DELETE_DATA_ANNOTATION,
    KubeConfigManager,
    PolicyError,
    STOP_ANNOTATION,
    VM_RUN_STRATEGY_RUNNING,
    VM_RUN_STRATEGY_STOPPED,
    run_intent,
    KUBEVIRT_GROUP,
    KUBEVIRT_VERSION,
    KUBEVIRT_VM_PLURAL,
    KUBEVIRT_VMI_PLURAL,
    CDI_GROUP,
    CDI_VERSION,
    CDI_DV_PLURAL,
)

logger = logging.getLogger("whistler.operator")

# Single ConfigManager shared across handlers. It loads in-cluster config and
# the mounted users/volumes/zones/images configuration once at startup.
_config_manager = None


def _get_config_manager() -> KubeConfigManager:
    global _config_manager
    if _config_manager is None:
        _config_manager = KubeConfigManager()
    return _config_manager


def _session_short_name(name: str, user: str) -> str:
    """CR names are `{user}-{session}`; ensure_session expects the short part."""
    prefix = f"{user}-"
    return name[len(prefix):] if name.startswith(prefix) else name


# Back-compat alias for the unit tests that imported the old name.
_instance_short_name = _session_short_name


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    level = os.environ.get("OPERATOR_LOG_LEVEL", "INFO").upper()
    logging.getLogger("whistler").setLevel(level)
    # `kopf run --verbose` sets the root logger to DEBUG, which turns on the
    # kubernetes client's per-request logging — including the body of every
    # Secret it reads (whistler/logsetup.py).
    quiet_chatty_libraries(level)
    # Instantiate eagerly so config-loading problems surface at startup.
    cm = _get_config_manager()
    # Seed the first admin account (whistler.bootstrapAdmin) if it doesn't
    # exist yet. Only the operator does this, to avoid server/portal racing
    # to create the same object.
    cm.ensure_bootstrap_admin()
    # Adopt pre-named-volumes home disks: record each as a HomeVolume and cut
    # its ownerReference to the Session, which would otherwise delete a user's
    # home along with the instance that happened to create it. Idempotent.
    try:
        adopted = cm.adopt_legacy_home_disks()
        if adopted:
            logger.info(f"Adopted {adopted} legacy home disk(s) as home volumes")
    except Exception as e:
        # Never fail startup for this: the operator not running is worse than
        # a home still carrying an ownerReference, which the next start retries.
        logger.error(f"Home-disk adoption failed (will retry next start): {e}")
    # Retain every home's PV as soon as it is bound, so an uninstall (which
    # deletes the user namespaces) cannot take the data with it
    # (design/backup.md, Phase 1). A plain thread rather than a kopf timer on
    # HomeVolume: kopf timers put a finalizer on every object they watch,
    # and a finalizer that only a running operator can remove would hang the
    # very namespace deletion this exists to survive.
    threading.Thread(target=_secure_user_data_loop, args=(cm,),
                     name="secure-user-data", daemon=True).start()
    # Push access-matrix changes to the dataset proxies' fencing policies
    # (the event handlers below wake it; the timeout is the backstop).
    threading.Thread(target=_refence_datasets_loop, args=(cm,),
                     name="refence-datasets", daemon=True).start()


# First pass right after startup (adoption has just run), then this often.
# Binding is asynchronous, so a home created a moment ago is caught on a
# later pass; the reconcile path also secures a claim it finds bound.
SECURE_USER_DATA_INTERVAL = 300
# While a claim exists but is not bound yet — a home just created, or the
# backup claim on a fresh install — its PV carries the class's policy, often
# Delete. Come back soon rather than leave that window open for five minutes.
SECURE_USER_DATA_RETRY = 15


def _secure_user_data_loop(cm, interval=SECURE_USER_DATA_INTERVAL,
                           retry=SECURE_USER_DATA_RETRY):
    while True:
        try:
            recorded = cm.secure_user_data()
            if recorded:
                logger.info(f"Recorded the retained disk of {recorded} "
                            f"home volume(s)")
        except Exception as e:
            logger.error(f"Securing user data failed (retrying): {e}")
        # A claim created since (secure_soon) or one seen unbound: soon.
        # Otherwise the slow pass. A claim created mid-wait cuts it short.
        wake = cm.secure_soon
        if getattr(cm, "unbound_claims", False) or wake.is_set():
            wake.clear()
            time.sleep(retry)
        else:
            wake.wait(interval)
            if wake.is_set():
                wake.clear()
                time.sleep(retry)   # give the new claim time to bind


# --------------------------------------------------------------------------- #
# Dataset fencing follows the access matrix (config.refence_dataset_proxies).  #
# The matrix is on User and Group CRs, which nothing used to watch, so a       #
# revoked cell waited for some session to be built before any proxy heard of   #
# it. Event handlers, not on.create/update/delete: they are stateless, so kopf #
# writes no annotation and no finalizer on a User or a Group. They only wake   #
# a single worker, which coalesces a burst (a group edit, the initial listing  #
# at startup) into one pass and keeps two passes from racing on one policy.    #
# --------------------------------------------------------------------------- #

REFENCE_DATASETS_INTERVAL = 300   # backstop for a missed watch event
REFENCE_DATASETS_SETTLE = 1       # let a burst of events land first
_refence_wake = threading.Event()


@kopf.on.event('whistler.martinmalmsten.net', 'v1', 'users')
@kopf.on.event('whistler.martinmalmsten.net', 'v1', 'groups')
@kopf.on.event('whistler.martinmalmsten.net', 'v1', 'datasets')
def access_changed_fn(**_):
    _refence_wake.set()


def _refence_datasets_loop(cm, wake=_refence_wake,
                           interval=REFENCE_DATASETS_INTERVAL,
                           settle=REFENCE_DATASETS_SETTLE, once=False):
    while True:
        wake.wait(interval)
        time.sleep(settle)
        # Cleared before the pass, so an edit that lands during it wakes the
        # next one instead of being absorbed by a pass that read before it.
        wake.clear()
        try:
            cm.refence_dataset_proxies()
        except Exception as e:
            logger.error(f"Re-fencing dataset proxies failed (retrying): {e}")
        # A deleted or archived Dataset fires the same events, so this is
        # also where its managed server goes (the data stays)...
        try:
            cm.prune_dataset_servers()
        except Exception as e:
            logger.error(f"Pruning dataset servers failed (retrying): {e}")
        # ...and a resurrected one's comes back (revive_dataset_servers says
        # why the kopf update handler cannot be trusted with that).
        try:
            cm.revive_dataset_servers()
        except Exception as e:
            logger.error(f"Reviving dataset servers failed (retrying): {e}")
        if once:
            return


# --------------------------------------------------------------------------- #
# Managed datasets: the portal writes the Dataset CR, the operator makes the   #
# claim and the server (config.ensure_managed_dataset). Built when the CR      #
# appears rather than on first mount, so a dataset exists to be filled before  #
# anyone is granted it. No on.delete (no finalizer). Archiving removes the     #
# server through the prune above and keeps the data; deleting an archived      #
# dataset for good is a mark (config.destroy_dataset), carried out here.       #
# --------------------------------------------------------------------------- #

def _marked(meta):
    return DELETE_DATA_ANNOTATION in (meta.get("annotations") or {})


def _served(spec, meta, **_):
    return (KubeConfigManager.is_managed_dataset(spec)
            and not KubeConfigManager.is_archived_dataset(spec)
            and not _marked(meta))


def _destroyed(spec, meta, **_):
    return KubeConfigManager.is_archived_dataset(spec) and _marked(meta)


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'datasets', when=_served)
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'datasets', when=_served)
@kopf.on.resume('whistler.martinmalmsten.net', 'v1', 'datasets', when=_served)
def managed_dataset_fn(name, spec, **_):
    if not _get_config_manager().ensure_managed_dataset(name, dict(spec)):
        raise kopf.TemporaryError(
            f"Managed dataset {name!r} is not served yet", delay=30)


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'datasets', when=_destroyed)
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'datasets', when=_destroyed)
@kopf.on.resume('whistler.martinmalmsten.net', 'v1', 'datasets', when=_destroyed)
def destroy_dataset_fn(name, **_):
    # Short delay: the usual reason to retry is a server pod still
    # terminating, which purge_dataset waits out before touching the fences.
    if not _get_config_manager().purge_dataset(name):
        raise kopf.TemporaryError(
            f"Dataset {name!r} is not deleted yet", delay=5)


# --------------------------------------------------------------------------- #
# Session lifecycle (unified ssh + desktop). The operator owns the pod/VM      #
# (and, for desktop, the per-session Service): reconcile provisions, the timer #
# drives Provisioning -> Booting -> Ready/Failed, and delete tears down.       #
#                                                                              #
# status.mode / status.runtime are denormalized by ensure_session so the timer #
# and delete handler dispatch (pod vs VMI, Service or not) without re-resolving #
# the template. KubeVirt may be absent — every VMI read is guarded so the      #
# operator runs without the KubeVirt CRDs installed.                           #
# --------------------------------------------------------------------------- #


def _session_address(name: str, namespace: str) -> str:
    """The per-session ClusterIP Service DNS name (Service name == CR name)."""
    return f"{name}.{namespace}.svc.cluster.local"


# Back-compat alias (unit-tested under the old name).
_desktop_address = _session_address


def _map_pod_phase(pod_phase: str, all_ready: bool) -> str:
    """Pure mapping from a pod's phase/readiness to a Session phase."""
    if pod_phase == "Running" and all_ready:
        return "Ready"
    if pod_phase == "Failed":
        return "Failed"
    return "Booting"


def _probe_pod(namespace, name, logger):
    """Return (session_phase, podName, address, message) by reading the
    session pod. `message` is None: a pod's failure reason is not surfaced
    here yet."""
    core = client.CoreV1Api()
    try:
        pod = core.read_namespaced_pod(name, namespace)
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Error reading pod {name}: {e}")
        # No pod: reconcile creates it synchronously, so by the time the timer
        # probes, an absent pod means the session was stopped (pod deleted, CR
        # kept), not "still provisioning".
        return ("Stopped", None, None, None)
    if pod.metadata.deletion_timestamp:
        return ("Terminating", name, None, None)
    phase = getattr(pod.status, "phase", None)
    statuses = pod.status.container_statuses or []
    all_ready = bool(statuses) and all(cs.ready for cs in statuses)
    session_phase = _map_pod_phase(phase, all_ready)
    address = _session_address(name, namespace) if session_phase == "Ready" else None
    return (session_phase, name, address, None)


def _vmi_condition(vmi, cond_type):
    """The status of one VMI condition ("True"/"False"/None if absent)."""
    for cond in (vmi.get("status") or {}).get("conditions") or []:
        if cond.get("type") == cond_type:
            return cond.get("status")
    return None


# KubeVirt `printableStatus` values that mean the VM is not going to boot on
# its own, each with the one-line reading a user can act on. Everything else
# KubeVirt prints for a VM with no VMI (Stopped, Provisioning, Starting,
# WaitingForVolumeBinding, ...) is either at rest or on its way, and the probe
# keeps treating those as it did.
#
# Before this table a VM in any of these states fell through to Booting and
# stayed there: `_probe_vm_without_vmi` only ever compared printableStatus
# against "Stopped". On 2026-09-05 a production VM failed eight times in a row
# (virt-handler could not see the kubelet root on k0s) and the portal said
# Starting for the whole hour, because KubeVirt's CrashLoopBackOff — the one
# word that would have said what was happening — was never read.
VM_ERROR_STATUSES = {
    "CrashLoopBackOff": "the VMI keeps failing to start and KubeVirt is backing off",
    "ErrorUnschedulable": "no node can take the VMI (resources, selectors, taints)",
    "ErrImagePull": "the root disk image cannot be pulled",
    "ImagePullBackOff": "the root disk image cannot be pulled",
    "ErrorPvcNotFound": "a PVC the VM refers to does not exist",
    "ErrorDataVolumeNotFound": "a DataVolume the VM refers to does not exist",
    "DataVolumeError": "the root disk import failed",
}


def _vm_failure_message(vm, printable, namespace, name) -> str:
    """One sentence per thing KubeVirt knows about why this VM is not running,
    for status.statusMessage. Reads only the VirtualMachine: its
    printableStatus, the start-failure counter virt-controller keeps under
    RerunOnFailure, and a `Failure` condition (how a webhook rejection of the
    VMI — a memory that is not a whole number of hugepages, say — reaches the
    VM object). Ends with the command that shows the rest."""
    status = (vm or {}).get("status") or {}
    parts = [f"KubeVirt reports {printable}: {VM_ERROR_STATUSES[printable]}."]
    failure = status.get("startFailure") or {}
    count = failure.get("consecutiveFailCount")
    if count:
        retry = failure.get("retryAfterTimestamp")
        parts.append(f"{count} consecutive failed start(s)"
                     + (f", next retry after {retry}" if retry else "") + ".")
    for cond in status.get("conditions") or []:
        if cond.get("type") == "Failure" and cond.get("message"):
            parts.append(cond["message"].rstrip(".") + ".")
    parts.append(f"See: kubectl -n {namespace} describe vm {name}")
    return " ".join(parts)


def _vmi_failure_message(vmi):
    """A Failed VMI's own reason, if it recorded one: the Synchronized
    condition is where virt-handler writes a sync error (a disk it could not
    prepare, a device it could not attach). None when there is nothing more
    to say than Failed."""
    for cond in (vmi.get("status") or {}).get("conditions") or []:
        if (cond.get("type") == "Synchronized" and cond.get("status") == "False"
                and cond.get("message")):
            return f"KubeVirt: {cond['message']}"
    return None


def _probe_vmi(namespace, name, logger):
    """Return (session_phase, vmiName, address, message) by reading the
    KubeVirt VMI. `message` is set only with a Failed phase — the reason, for
    status.statusMessage.

    Every read is 404-guarded so the timer never crashes on a cluster without
    the KubeVirt (or CDI) CRDs. A missing VMI is disambiguated by falling back
    to the VirtualMachine (halted -> Stopped) and the CDI root-disk DataVolume
    (still importing -> Importing) rather than reporting Booting forever."""
    api = client.CustomObjectsApi()
    try:
        vmi = api.get_namespaced_custom_object(
            KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, KUBEVIRT_VMI_PLURAL, name
        )
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Error reading VMI {name}: {e}")
            return ("Booting", name, None, None)
        return _probe_vm_without_vmi(api, namespace, name, logger)
    if (vmi.get("metadata") or {}).get("deletionTimestamp"):
        # Draining after a stop (runStrategy Halted) or a session delete —
        # without this the VMI still reports phase Running while the guest
        # shuts down and the dashboard would keep saying Running.
        return ("Terminating", name, None, None)
    phase = (vmi.get("status") or {}).get("phase")
    if phase == "Running":
        # Running means the domain booted, NOT that the guest serves anything —
        # cloud-init still has ~20s of work before the streamer/sshd listens.
        # Reporting Ready here sent the portal's connect page at a dead port and
        # produced "desktop backend unreachable" on every cold start. The VMI's
        # Ready condition carries the readinessProbe result (_build_vm_spec sets
        # one), so gate on that instead. A VMI with no probe has Ready=True from
        # the moment it runs, so this stays correct for VMs created before the
        # probe existed — they just keep the old racy behaviour until restarted.
        if _vmi_condition(vmi, "Ready") == "True":
            return ("Ready", name, _session_address(name, namespace), None)
        return ("Booting", name, None, None)
    if phase == "Failed":
        return ("Failed", name, None, _vmi_failure_message(vmi))
    if phase == "Succeeded":
        # The domain exited gracefully — the guest shut itself down (Power Off
        # in the desktop menu, `poweroff`), or Whistler halted the VM and it
        # obeyed. Under VM_RUN_STRATEGY_RUNNING the first of those sticks
        # instead of being restarted, and the timer records it as a stop.
        return ("Stopped", name, None, None)
    return ("Booting", name, None, None)


def _probe_vm_without_vmi(api, namespace, name, logger):
    """Disambiguate a missing VMI: Stopped (VM absent, halted, or shut down
    from inside the guest), Failed (KubeVirt's own status says it will not
    boot — VM_ERROR_STATUSES, with the reason as the fourth element),
    Importing (CDI still pulling the root disk), else Booting (VMI not up
    yet)."""
    try:
        vm = api.get_namespaced_custom_object(
            KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, KUBEVIRT_VM_PLURAL, name
        )
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Error reading VM {name}: {e}")
            return ("Booting", name, None, None)
        return ("Stopped", None, None, None)
    if (vm.get("spec") or {}).get("runStrategy") == VM_RUN_STRATEGY_STOPPED:
        return ("Stopped", name, None, None)
    if _guest_powered_off(vm):
        # Not halted by us and no VMI under it: the guest shut itself down
        # (RerunOnFailure leaves it that way — VM_RUN_STRATEGY_RUNNING). Before
        # this, that state fell through to Booting and stuck there forever,
        # because nothing was ever going to boot.
        return ("Stopped", name, None, None)
    printable = (vm.get("status") or {}).get("printableStatus")
    if printable in VM_ERROR_STATUSES:
        # Same shape of bug as the power-off above, one row down: KubeVirt has
        # said in so many words that nothing is coming, and Booting would hide
        # it. Checked before the DataVolume so an import error is not reported
        # as Importing.
        return ("Failed", name, None,
                _vm_failure_message(vm, printable, namespace, name))
    try:
        dv = api.get_namespaced_custom_object(
            CDI_GROUP, CDI_VERSION, namespace, CDI_DV_PLURAL, f"{name}-root"
        )
        if (dv.get("status") or {}).get("phase") != "Succeeded":
            return ("Importing", name, None, None)
    except client.rest.ApiException:
        pass  # no DataVolume (containerDisk boot) or CDI absent
    return ("Booting", name, None, None)


def _guest_powered_off(vm) -> bool:
    """Whether this VirtualMachine is stopped because the guest shut *itself*
    down — Power Off in the desktop menu, `sudo poweroff`, a `shutdown` that
    reached its deadline.

    Three readings of the VM's own status, and each one excludes a state that
    would otherwise be mistaken for this:

    - `status.runStrategy` is the strategy virt-controller has **observed**,
      not the one this reconcile just asked for. It is what separates a guest
      power-off from a start still in flight: a VM Whistler is starting says
      Halted here until the controller catches up, and a VM Whistler has
      stopped says Halted for good.
    - `printableStatus == "Stopped"` is KubeVirt's own answer to "is anything
      running", and the only one that accounts for its start expectations:
      between deciding to create a VMI and that object existing it says
      Starting, where a bare "is there a VMI" test would say stopped and
      restart the machine out from under itself. It also covers the ~10s in
      which a Succeeded VMI has not been reaped yet, so the stop is recorded
      on the first tick rather than the second.
    - an empty `stateChangeRequests` means KubeVirt is not itself about to
      start this VM. That is the difference between a guest that chose to shut
      down and one that crashed: RerunOnFailure answers a Failed VMI by
      queueing a start, and calling that a stop would take away the automatic
      restart that is the whole reason for the strategy.

    Fails closed on anything unexpected (a field absent, a status KubeVirt
    words differently): the cost of a false negative is a session that reads
    Stopped while its CR still intends to run, which the next start fixes. A
    false positive stops a machine nobody asked to stop."""
    status = (vm or {}).get("status") or {}
    return (status.get("runStrategy") == VM_RUN_STRATEGY_RUNNING
            and status.get("printableStatus") == "Stopped"
            and not status.get("stateChangeRequests"))


def _record_guest_shutdown(namespace, name, patch, logger) -> None:
    """Turn a guest's own power-off into the same stop the portal button makes.

    Without this the Session CR would go on saying "run" (last-connect newer
    than last-stop) while the machine is off, and the two halves of that
    disagreement both bite. Any later reconcile that happens to touch the CR —
    an admin editing overrides, a viewer page nudging the instance — would
    read the intent and boot the guest back up, which is the exact bug the two
    annotations were introduced to kill. And the *next* deliberate start would
    be a no-op: KubeVirt only acts on a runStrategy patch that changes
    something, and a VM the guest halted still says RerunOnFailure (see
    VM_RUN_STRATEGY_RUNNING). Writing the stop mark fixes both — the following
    reconcile puts the spec back to Halted, which re-arms that transition.

    The write is an annotation on the CR, so it goes through the same path a
    portal stop does and needs no new privilege anywhere.

    Reads the VirtualMachine itself rather than trusting the probed phase:
    Stopped is also what a VM reports in the moments before a start lands, and
    turning that into a stop would cancel the start that was on its way."""
    try:
        vm = client.CustomObjectsApi().get_namespaced_custom_object(
            KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, KUBEVIRT_VM_PLURAL, name
        )
    except client.rest.ApiException as e:
        if e.status != 404:
            logger.warning(f"Error reading VM {name}: {e}")
        return
    if not _guest_powered_off(vm):
        return
    logger.info(f"Session {name}: guest shut itself down; recording a stop")
    patch.meta['annotations'] = {STOP_ANNOTATION: str(time.time())}


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'sessions')
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'sessions')
def reconcile_session_fn(spec, name, namespace, meta, patch, logger, **kwargs):
    """Own session lifecycle: ensure the pod/VM (and, for desktop, the
    per-session Service) exist. The phase machine itself runs in the timer.

    Consumers create/patch the Session CR rather than creating pods directly,
    making this the single owner of pod creation. Note there is deliberately no
    `@kopf.on.resume` handler: pods are created on-demand (CR creation, or the
    `whistler/last-connect` annotation bump when a user connects), not when the
    operator/cluster (re)starts — a session whose pod was stopped stays stopped
    until someone connects again. The phase timer still resumes on its own."""
    if meta.get('deletionTimestamp'):
        logger.info(f"Skipping reconcile for deleting session {name}")
        return

    user = spec.get('user')
    if not user:
        logger.warning(f"Session {name} has no spec.user; cannot reconcile")
        return

    session_name = _session_short_name(name, user)
    cm = _get_config_manager()

    logger.info(f"Reconciling session {name}: ensuring workload exists")
    try:
        result = cm.ensure_session(user, session_name)
    except PolicyError as e:
        # Hard, non-retryable: the template violates an operator policy. No
        # workload was created, so mark this a terminal policy failure with its
        # reason — the phase timer keys off policyFailed to leave this Failed
        # alone instead of probing a nonexistent pod/VMI and reporting Stopped
        # (which masked exactly this: image not in the vm allow-list).
        logger.error(f"Session {name} rejected by policy: {e}")
        # A stop is never a policy question. ensure_session builds the whole
        # workload before it gets to the run state, so the refusal arrives
        # before the halt does. Without this, a running VM whose saved
        # defaults can no longer be honoured (a GPU type that is gone) shows
        # Failed and ignores every stop.
        if not run_intent(meta.get('annotations')):
            cm.halt_session_workload(user, session_name)
        patch.status['phase'] = 'Failed'
        patch.status['statusMessage'] = str(e)
        patch.status['policyFailed'] = True
        return

    # Past policy: clear any prior policy-failure marker so a fixed template
    # recovers (the timer resumes probing once policyFailed is False). A
    # reconnect re-fires this handler, which is how a Failed session comes back.
    patch.status['policyFailed'] = False
    patch.status['statusMessage'] = None

    # Denormalize mode/runtime into status (when known) so the timer and delete
    # handler can dispatch without re-resolving the template.
    if result.get('mode'):
        patch.status['mode'] = result['mode']
    if result.get('runtime'):
        patch.status['runtime'] = result['runtime']
    if result.get('displayPort') is not None:
        patch.status['displayPort'] = result['displayPort']
    if result.get('viewer'):
        patch.status['viewer'] = result['viewer']

    if result['ok']:
        # ensure_session may report a more accurate initial phase (a VM
        # created Halted starts life Stopped, not Provisioning).
        patch.status['phase'] = result.get('phase') or 'Provisioning'
    else:
        # Transient (e.g. the old pod is still terminating, or the template
        # isn't present yet). Retry with backoff.
        patch.status['phase'] = 'Pending'
        raise kopf.TemporaryError(f"Workload for {name} could not be created yet", delay=2)


@kopf.on.timer('whistler.martinmalmsten.net', 'v1', 'sessions', interval=10)
def session_phase_timer(spec, name, namespace, meta, status, patch, logger, **kwargs):
    """Drive the Session phase machine by probing the child pod/VMI."""
    if meta.get('deletionTimestamp'):
        return
    status = status or {}
    if not status.get('phase'):
        return  # not reconciled yet
    if status.get('policyFailed'):
        # Terminal policy rejection owned by the reconcile handler — there is no
        # workload to probe, and overwriting its Failed with a probe result
        # would report a misleading Stopped. Leave phase/statusMessage intact.
        return

    runtime = status.get('runtime')
    if runtime == 'vm':
        new_phase, child_name, address, message = _probe_vmi(namespace, name, logger)
        patch.status['vmiName'] = child_name
        # A VM that is down while the CR still says run is either a guest that
        # powered itself off or a start still on its way; only the first is a
        # stop, and only the VM's own status can tell them apart.
        if new_phase == 'Stopped' and run_intent(meta.get('annotations')):
            _record_guest_shutdown(namespace, name, patch, logger)
    else:
        new_phase, child_name, address, message = _probe_pod(namespace, name, logger)
        patch.status['podName'] = child_name

    patch.status['phase'] = new_phase
    # A Failed phase carries its reason in the same field the reconcile handler
    # uses for a policy refusal — the gateway prints it when a connect fails,
    # and `get_user_instances` hands it to the portal. Cleared on any other
    # phase so a VM that recovers does not keep last week's reason; the policy
    # message is safe because policyFailed returned above.
    patch.status['statusMessage'] = message if new_phase == 'Failed' else None
    # Only desktop sessions have a Service / reachable address; ssh sessions are
    # bridged via `kubectl exec` and have no Service to point at.
    if status.get('mode') == 'desktop' and address:
        patch.status['address'] = address


@kopf.on.delete('whistler.martinmalmsten.net', 'v1', 'sessions')
def delete_session_fn(spec, name, namespace, status, logger, **kwargs):
    """Tear down the pod/VM (and, for desktop, the per-session Service).
    ownerReferences also GC these; the explicit deletes are for promptness."""
    logger.info(f"Deleting session {name}")
    status = status or {}
    core = client.CoreV1Api()

    if status.get('mode') == 'desktop':
        try:
            core.delete_namespaced_service(name, namespace)
            logger.info(f"Deleted service {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete service {name}: {e}")

    runtime = status.get('runtime')

    def _delete_pod():
        try:
            core.delete_namespaced_pod(name, namespace)  # pod name == CR name
            logger.info(f"Deleted pod {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete pod {name}: {e}")

    def _delete_vm():
        try:
            client.CustomObjectsApi().delete_namespaced_custom_object(
                KUBEVIRT_GROUP, KUBEVIRT_VERSION, namespace, KUBEVIRT_VM_PLURAL, name
            )
            logger.info(f"Deleted VirtualMachine {name}")
        except client.rest.ApiException as e:
            if e.status != 404:
                logger.warning(f"Failed to delete VirtualMachine {name}: {e}")

    if runtime == 'vm':
        _delete_vm()
    elif runtime in ('container', 'kata'):
        _delete_pod()
    else:
        # status.runtime never written (e.g. failed before reconcile set it):
        # try both so nothing is orphaned.
        _delete_pod()
        _delete_vm()


# --------------------------------------------------------------------------- #
# Deleting a home with its data (config.delete_home_volume marks, this acts). #
# The PV is Retain, so the data only goes once it is set back to Delete — a    #
# cluster-scoped write that stays in the operator. Create/update/resume only:  #
# no on.delete, so kopf puts no finalizer on HomeVolumes.                      #
# --------------------------------------------------------------------------- #

_PURGE_FILTER = dict(annotations={DELETE_DATA_ANNOTATION: kopf.PRESENT})


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'homevolumes', **_PURGE_FILTER)
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'homevolumes', **_PURGE_FILTER)
@kopf.on.resume('whistler.martinmalmsten.net', 'v1', 'homevolumes', **_PURGE_FILTER)
def purge_home_volume_fn(name, namespace, **_):
    if not _get_config_manager().purge_home_volume(namespace, name):
        raise kopf.TemporaryError(
            f"Could not delete home volume {namespace}/{name} yet", delay=30)
    logger.info(f"Deleted home volume {namespace}/{name} and its data")


# Archiving a home and restoring one (config.py, "Archived homes"): the portal
# marks, the operator moves the PV between claims. Retried with a short delay
# because a PV takes a moment to go Released once its claim is deleted.
_ARCHIVE_FILTER = dict(annotations={ARCHIVE_ANNOTATION: kopf.PRESENT})


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'homevolumes', **_ARCHIVE_FILTER)
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'homevolumes', **_ARCHIVE_FILTER)
@kopf.on.resume('whistler.martinmalmsten.net', 'v1', 'homevolumes', **_ARCHIVE_FILTER)
def archive_home_volume_fn(name, namespace, **_):
    if not _get_config_manager().archive_home_volume(namespace, name):
        raise kopf.TemporaryError(
            f"Archiving home volume {namespace}/{name} is not done yet", delay=5)


def _restoring(spec, **_):
    return bool(spec.get("fromArchive"))


@kopf.on.create('whistler.martinmalmsten.net', 'v1', 'homevolumes', when=_restoring)
@kopf.on.update('whistler.martinmalmsten.net', 'v1', 'homevolumes', when=_restoring)
@kopf.on.resume('whistler.martinmalmsten.net', 'v1', 'homevolumes', when=_restoring)
def unarchive_home_volume_fn(name, namespace, **_):
    if not _get_config_manager().unarchive_home_volume(namespace, name):
        raise kopf.TemporaryError(
            f"Restoring home volume {namespace}/{name} is not done yet", delay=5)
