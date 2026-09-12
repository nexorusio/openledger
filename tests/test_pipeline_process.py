"""Real local-process tests for stubborn collectors and orphan descendants."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

PATH = Path(__file__).resolve().parents[1] / "maigret/web/pipeline_process.py"
spec = importlib.util.spec_from_file_location("pipeline_process_test", PATH)
processes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(processes)
pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Production process supervision targets Linux"
)


def run(code, *, cancelled=lambda: False, timeout=3, max_output=4096):
    return asyncio.run(
        processes.run_bounded_process(
            [sys.executable, "-c", code],
            b"test-input",
            timeout_seconds=timeout,
            cancelled=cancelled,
            max_output_bytes=max_output,
        )
    )


def wait_until_stopped(pids, *, timeout=3):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if all(not processes.process_is_alive(pid) for pid in pids):
            return
        time.sleep(0.025)
    pytest.fail("Collector descendants remained alive after bounded cleanup")


def stubborn_tree(tmp_path):
    record = tmp_path / "pids.json"
    script = """
import json, os, pathlib, signal, subprocess, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, '-c', 'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(90)'])
pathlib.Path(sys.argv[1]).write_text(json.dumps([os.getpid(), child.pid]))
while True: time.sleep(0.05)
"""
    path = tmp_path / "stubborn.py"
    path.write_text(script)
    return record, [sys.executable, str(path), str(record)]


def test_bounded_process_preserves_output_and_input():
    output, diagnostic, code = run(
        'import sys;print(sys.stdin.read());print("diagnostic",file=sys.stderr)'
    )
    assert code == 0 and output == b"test-input\n" and diagnostic == b"diagnostic\n"


def test_output_overflow_is_bounded_and_process_killed():
    started = time.monotonic()
    with pytest.raises(processes.CollectorProcessError, match="bounded output"):
        run(
            'import sys,time;print("x"*10000,flush=True);time.sleep(90)', max_output=128
        )
    assert time.monotonic() - started < 2


def test_timeout_kills_stubborn_process_and_grandchild(tmp_path):
    record, command = stubborn_tree(tmp_path)
    started = time.monotonic()
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(
            processes.run_bounded_process(
                command, b"", timeout_seconds=0.35, cancelled=lambda: False
            )
        )
    assert time.monotonic() - started < 2
    wait_until_stopped(json.loads(record.read_text()))


def test_operator_stop_kills_entire_group_before_return(tmp_path):
    record, command = stubborn_tree(tmp_path)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            processes.run_bounded_process(
                command, b"", timeout_seconds=10, cancelled=record.exists
            )
        )
    wait_until_stopped(json.loads(record.read_text()))


def test_cancelling_parent_coroutine_still_drains_group(tmp_path):
    record, command = stubborn_tree(tmp_path)

    async def scenario():
        task = asyncio.create_task(
            processes.run_bounded_process(
                command, b"", timeout_seconds=10, cancelled=lambda: False
            )
        )
        while not record.exists():
            await asyncio.sleep(0.025)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    wait_until_stopped(json.loads(record.read_text()))


def test_success_cannot_leave_detached_work_in_same_session(tmp_path):
    record = tmp_path / "child.pid"
    code = (
        'import pathlib,subprocess,sys; c=subprocess.Popen([sys.executable,"-c","import time;time.sleep(90)"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);pathlib.Path('
        + repr(str(record))
        + ').write_text(str(c.pid));print("done")'
    )
    output, _, code = run(code)
    assert output == b"done\n" and code == 0
    wait_until_stopped([int(record.read_text())])


def test_parent_death_watchdog_kills_grandchildren_even_when_collector_ignores_signals(
    tmp_path,
):
    # The supervisor is killed with SIGKILL, so its coroutine/finally handlers
    # cannot run. The child watchdog must independently clear all collection.
    record = tmp_path / "guarded.json"
    child = tmp_path / "guarded_child.py"
    child.write_text("""import importlib.util,json,os,pathlib,signal,subprocess,sys,time
spec=importlib.util.spec_from_file_location('guard',sys.argv[1]);guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
watcher=guard._start_parent_watchdog(int(sys.argv[2]))
# Provider ignores ordinary termination and spends forever in its own loop.
signal.signal(signal.SIGTERM,signal.SIG_IGN)
grandchild=subprocess.Popen([sys.executable,'-c','import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(90)'])
pathlib.Path(sys.argv[3]).write_text(json.dumps([os.getpid(),grandchild.pid,watcher.pid]))
while True: pass
""")
    supervisor = tmp_path / "supervisor.py"
    supervisor.write_text("""import importlib.util,os,subprocess,sys,time
spec=importlib.util.spec_from_file_location('guard',sys.argv[1]);guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
subprocess.Popen([sys.executable,sys.argv[2],sys.argv[1],str(os.getpid()),sys.argv[3]],start_new_session=True)
time.sleep(90)
""")
    parent = subprocess.Popen(
        [sys.executable, str(supervisor), str(PATH), str(child), str(record)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    pids = []
    try:
        until = time.monotonic() + 5
        while not record.exists() and time.monotonic() < until:
            time.sleep(0.025)
        if not record.exists():
            parent.kill()
            _, diagnostic = parent.communicate(timeout=2)
            pytest.fail("Guarded collector did not start: " + diagnostic.decode())
        pids = json.loads(record.read_text())
        parent.kill()
        parent.wait(timeout=2)
        wait_until_stopped(pids)
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait()
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_parent_identity_mismatch_refuses_guard_before_collection():
    with pytest.raises(processes.CollectorProcessError, match="no longer alive"):
        processes._start_parent_watchdog(os.getpid())
