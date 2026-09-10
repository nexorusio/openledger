# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
"""One killable optional artifact conversion; the caller owns publication."""

import ctypes
import json
import multiprocessing
import os
import signal
import time


def _render(connection, general_results, usernames, session_key, kwargs, config):
    # Keep this child in its collector's dedicated session.  The collector
    # supervisor can therefore terminate an artifact child even if this child
    # exits before a converter grandchild does.  Its own group remains separate
    # so standalone callers can kill it without touching their process group.
    if os.name == "posix":
        os.setpgid(0, 0)
    try:
        from maigret.web.app import app, build_reports

        app.config.update(config)
        result = build_reports(general_results, usernames, session_key, **kwargs)
        # Keep IPC frames atomic and tiny. Receiving a large pickled result
        # after poll() could otherwise block past the conversion deadline.
        with open(os.path.join(kwargs['reports_root'], '.rendered-result.json'), 'w') as output:
            json.dump(result, output)
        connection.send_bytes(b'1')
    except BaseException:
        connection.send_bytes(b'0')
    finally:
        connection.close()


def enable_subreaper():
    """Linux supervisors reap orphaned descendants as well as direct children."""
    if os.path.isdir('/proc/self'):
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            raise RuntimeError('Cannot establish descendant cleanup supervision')


def _proc_records():
    """Return ``pid -> (parent, group, session)`` from Linux procfs."""
    records = {}
    if not os.path.isdir('/proc'):
        return records
    try:
        entries = os.scandir('/proc')
        namespace = os.readlink('/proc/self/ns/pid')
        translate = int(os.readlink('/proc/self')) != os.getpid()
    except OSError:
        return records
    translated_ids = {}
    with entries:
        for entry in entries:
            if not entry.name.isdigit():
                continue
            try:
                if translate and os.readlink(entry.path + '/ns/pid') != namespace:
                    continue
                with open(entry.path + '/stat') as record:
                    fields = record.read().rsplit(')', 1)[1].split()
                # After pid and comm are removed: state, ppid, pgrp, session.
                pid, parent, group, session = int(entry.name), int(fields[1]), int(fields[2]), int(fields[3])
                if translate:
                    # Some test/container runtimes mount procfs from an outer
                    # PID namespace. killpg/waitpid require IDs in our namespace.
                    with open(entry.path + '/status') as status_file:
                        status = dict(line.split(':', 1) for line in status_file if ':' in line)
                    translated_ids[pid] = int(status['NSpid'].split()[-1])
                    group = int(status['NSpgid'].split()[-1])
                    session = int(status['NSsid'].split()[-1])
                records[pid] = (parent, group, session)
            except (OSError, ValueError, IndexError, KeyError):
                continue
    if translate:
        records = {translated_ids[pid]: (translated_ids.get(parent, 0), group, session)
                   for pid, (parent, group, session) in records.items() if pid in translated_ids}
    return records


def descendant_process_groups(root_pid):
    """Snapshot PGIDs for ``root_pid`` and every currently visible descendant."""
    if os.name != 'posix':
        return set()
    records = _proc_records()
    children = {}
    for pid, (parent, _group, _session) in records.items():
        children.setdefault(parent, set()).add(pid)
    pending, seen, groups = [root_pid], set(), set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        record = records.get(pid)
        if record is None:
            continue
        _parent, group, _session = record
        groups.add(group)
        pending.extend(children.get(pid, ()))
    groups.discard(os.getpgrp())
    return groups


def _session_process_groups(session_id):
    groups = {
        group
        for _pid, (_parent, group, session) in _proc_records().items()
        if session == session_id
    }
    groups.discard(os.getpgrp())
    return groups


def isolated_session_id():
    """Return the current session id only when this process owns it."""
    if os.name != 'posix':
        return None
    try:
        session_id = os.getsid(0)
    except OSError:
        return None
    return session_id if session_id == os.getpid() else None


def reap_process(process, *, grace=1.0, whole_session=False, session_id=None,
                 extra_groups=()):
    """Stop tracked process groups and join the direct child.

    ``whole_session`` is allowed only for a dedicated execution session.  A
    normal artifact renderer shares its collector session, so callers pass the
    collector session id only after source drain.  Standalone callers instead
    supply the cumulative descendant PGIDs observed while the direct child was
    alive.
    """
    groups = set(extra_groups)
    if os.name == 'posix':
        groups.update(descendant_process_groups(process.pid))
        if whole_session:
            groups.update(_session_process_groups(session_id or process.pid))
        groups.discard(os.getpgrp())
    for group in groups:
        try:
            os.killpg(group, signal.SIGTERM)
        except ProcessLookupError:
            pass
    if process.is_alive():
        process.terminate()
    process.join(grace)
    if os.name == 'posix':
        groups.update(descendant_process_groups(process.pid))
        if whole_session:
            groups.update(_session_process_groups(session_id or process.pid))
        groups.discard(os.getpgrp())
    for group in groups:
        try:
            os.killpg(group, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process.is_alive():
        process.kill()
        process.join(grace)
    if process.is_alive():
        raise RuntimeError('Execution child did not exit after termination')
    process.join()
    # Reap adopted grandchildren only from this execution's groups.
    deadline = time.monotonic() + grace
    for group in groups:
        while time.monotonic() < deadline:
            try:
                child, _ = os.waitpid(-group, os.WNOHANG)
            except ChildProcessError:
                break
            if child == 0:
                time.sleep(0.01)


def build_bounded_artifacts(general_results, usernames, session_key, *, kwargs,
                            config, stop_check=None, timeout=30.0):
    """Return rendered metadata or None; never write a terminal DB/file record."""
    enable_subreaper()
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_render, args=(
        sender, general_results, usernames, session_key, kwargs, config,
    ), name="openledger-artifacts")
    deadline = time.monotonic() + max(0.1, min(float(timeout), 120.0))
    # A collector owns a dedicated session after source drain.  Sweeping its
    # other groups handles a direct artifact child that exits before its nested
    # converter does.  A standalone caller never sweeps its shared session.
    session_id = isolated_session_id()
    known_groups = set()
    process.start()
    sender.close()
    try:
        while time.monotonic() < deadline:
            known_groups.update(descendant_process_groups(process.pid))
            if stop_check and stop_check():
                return None
            if receiver.poll(0.05):
                try:
                    if receiver.recv_bytes(maxlength=1) != b'1':
                        return None
                    path = os.path.join(kwargs['reports_root'], '.rendered-result.json')
                    with open(path, 'rb') as result_file:
                        content = result_file.read(32 * 1024 * 1024 + 1)
                    if len(content) > 32 * 1024 * 1024:
                        return None
                    return json.loads(content)
                except (EOFError, OSError, ValueError):
                    return None
            if not process.is_alive():
                return None
        return None
    finally:
        reap_process(
            process,
            whole_session=session_id is not None,
            session_id=session_id,
            extra_groups=known_groups,
        )
        receiver.close()
