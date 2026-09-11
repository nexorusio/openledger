"""Offline Linux process-group cleanup boundary."""

import multiprocessing
import os
from pathlib import Path
import subprocess
import sys

import pytest

from maigret.web.artifact_execution import (
    descendant_process_groups,
    enable_subreaper,
    reap_process,
)


def _spawn_separate_group_descendant(connection):
    # The direct child exits while its descendant deliberately leaves its group.
    os.setpgid(0, 0)
    descendant = subprocess.Popen(
        [
            sys.executable,
            '-c',
            'import time; time.sleep(60)',
        ], preexec_fn=os.setpgrp,
    )
    connection.send((descendant.pid, os.getpgid(descendant.pid)))
    connection.recv()  # The supervisor has captured the new group before exit.
    connection.close()


@pytest.mark.skipif(
    os.name != 'posix' or not Path('/proc').exists(),
    reason='Linux process cleanup',
)
def test_reaper_stops_tracked_separate_group_after_direct_child_exit():
    enable_subreaper()
    context = multiprocessing.get_context('spawn')
    reader, writer = context.Pipe(True)
    process = context.Process(target=_spawn_separate_group_descendant, args=(writer,))
    process.start()
    writer.close()
    assert reader.poll(5)
    descendant, descendant_group = reader.recv()
    assert descendant_group in descendant_process_groups(process.pid)
    reader.send('tracked')
    process.join(5)
    assert not process.is_alive()
    reap_process(process, extra_groups={descendant_group})
    with pytest.raises(ProcessLookupError):
        os.kill(descendant, 0)
    reader.close()
