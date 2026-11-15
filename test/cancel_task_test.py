# -*- coding: utf-8 -*-
#
# Copyright 2026 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import signal
import tempfile
import threading
import time

from helpers import LuigiTestCase

import luigi
import luigi.rpc
import luigi.scheduler
import luigi.worker
import server_test
from luigi.scheduler import Scheduler, RPC_METHODS
from luigi.task_status import (
    BATCH_RUNNING, CANCEL_REQUESTED, CANCELLED, DISABLED, DONE, FAILED,
    PENDING, RUNNING,
)
from luigi.worker import Worker

WORKER = 'worker-1'


def wait_for(condition, timeout=15.0, interval=0.02):
    """Poll ``condition`` until it holds; returns False on timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def read_pid(path):
    try:
        with open(path) as f:
            return int(f.read())
    except (OSError, ValueError):
        return None


def make_scheduler(tmpdir, **kwargs):
    kwargs.setdefault('state_path', os.path.join(tmpdir, 'state.pickle'))
    return Scheduler(**kwargs)


def add_capable_worker(sch, worker=WORKER):
    sch.add_worker(worker, [('cancel_support', True)])


def start_task(sch, task_id, worker=WORKER, **add_kwargs):
    """Schedule task and dispatch it so that it is RUNNING on worker."""
    sch.add_task(task_id=task_id, worker=worker, **add_kwargs)
    reply = sch.get_work(worker=worker, host='testhost')
    assert reply['task_id'] == task_id, reply
    return reply


def task_status(sch, task_id):
    task = sch._state.get_task(task_id)
    return task.status if task is not None else None


class _FileControlledTask(luigi.Task):
    """
    Task whose run() announces its pid through a file and then blocks until
    a release file appears, finally writing its output.

    accepts_messages is left at its default (False) on purpose: cancellation
    must not depend on the task accepting scheduler messages.
    """
    started_file = luigi.Parameter()
    release_file = luigi.Parameter()
    done_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.done_file)

    def run(self):
        with open(self.started_file, 'w') as f:
            f.write(str(os.getpid()))
        while not os.path.exists(self.release_file):
            time.sleep(0.02)
        with self.output().open('w') as f:
            f.write('done')


class _TermResistantTask(_FileControlledTask):
    """A _FileControlledTask that ignores SIGTERM (must be SIGKILLed)."""

    resources = {'cancel_test_resource': 1}

    def run(self):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        super(_TermResistantTask, self).run()


class _QuickTask(luigi.Task):
    done_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.done_file)

    def run(self):
        with self.output().open('w') as f:
            f.write('done')


class _ResourceTask(_QuickTask):
    resources = {'cancel_test_resource': 1}


class _MainProcessTask(_FileControlledTask):
    """Forces in-process execution even when the worker has a timeout set."""
    worker_timeout = 0


class _WorkerThread(threading.Thread):
    """Runs a worker, capturing its result and any exception."""

    def __init__(self, worker):
        super(_WorkerThread, self).__init__(daemon=True)
        self.worker = worker
        self.result = None
        self.error = None

    def run(self):
        try:
            self.result = self.worker.run()
        except BaseException as ex:
            self.error = ex


class CancelTaskSchedulerApiTest(LuigiTestCase):
    """Scheduler-level tests of the cancel_task RPC and its state machine."""

    def setUp(self):
        super(CancelTaskSchedulerApiTest, self).setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.sch = make_scheduler(self._tmp.name)

    def test_cancel_unknown_task(self):
        res = self.sch.cancel_task('no-such-task')
        self.assertEqual(res, {'task_id': 'no-such-task', 'accepted': False, 'status': None})

    def test_cancel_pending_task_is_immediately_cancelled(self):
        self.sch.add_task(task_id='A', worker=WORKER, family='F')
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': True, 'status': CANCELLED})
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)
        # a cancelled pending task must not be dispatched
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])
        # and is visible through the listing APIs
        self.assertIn('A', self.sch.task_list(status=CANCELLED))
        self.assertIn('A', self.sch.task_search('A')[CANCELLED])
        self.assertEqual(self.sch.dep_graph('A')['A']['status'], CANCELLED)

    def test_cancel_done_or_failed_task_rejected(self):
        self.sch.add_task(task_id='A', worker=WORKER, status=DONE)
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': DONE})
        self.assertEqual(task_status(self.sch, 'A'), DONE)

        self.sch.add_task(task_id='B', worker=WORKER)
        self.sch.add_task(task_id='B', worker=WORKER, status=FAILED)
        res = self.sch.cancel_task('B')
        self.assertEqual(res, {'task_id': 'B', 'accepted': False, 'status': FAILED})
        self.assertEqual(task_status(self.sch, 'B'), FAILED)

    def test_cancel_disabled_task_rejected(self):
        self.sch.add_task(task_id='A', worker=WORKER, status=DISABLED)
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': DISABLED})
        self.assertEqual(task_status(self.sch, 'A'), DISABLED)

    def test_cancel_running_task(self):
        add_capable_worker(self.sch)
        self.sch.add_task(task_id='A', worker=WORKER, family='F')
        reply = self.sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual(reply['task_id'], 'A')

        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': True, 'status': CANCEL_REQUESTED})
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)
        # the new state is visible through the listing APIs
        self.assertIn('A', self.sch.task_list(status=CANCEL_REQUESTED))
        self.assertIn('A', self.sch.task_search('A')[CANCEL_REQUESTED])
        self.assertEqual(self.sch.dep_graph('A')['A']['status'], CANCEL_REQUESTED)

        # repeated cancel: same status reported, no second effect
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': CANCEL_REQUESTED})
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)

    def test_cancel_running_task_without_worker_capability_rejected(self):
        # worker did not report cancel_support (e.g. runs tasks in its main process)
        self.sch.add_worker(WORKER, [('cancel_support', False)])
        start_task(self.sch, 'A')
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': RUNNING})
        self.assertEqual(task_status(self.sch, 'A'), RUNNING)

    def test_cancel_running_task_without_worker_info_rejected(self):
        start_task(self.sch, 'A')  # worker never announced any info
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': RUNNING})
        self.assertEqual(task_status(self.sch, 'A'), RUNNING)

    def test_cancel_batched_task_rejected(self):
        add_capable_worker(self.sch)
        self.sch.add_task_batcher(worker=WORKER, task_family='F', batched_args=['a'], max_batch_size=3)
        self.sch.add_task(task_id='F(a=1)', worker=WORKER, family='F', params={'a': '1'}, batchable=True)
        self.sch.add_task(task_id='F(a=2)', worker=WORKER, family='F', params={'a': '2'}, batchable=True)
        reply = self.sch.get_work(worker=WORKER, host='testhost')
        self.assertIn('batch_task_ids', reply)
        self.assertEqual(task_status(self.sch, 'F(a=1)'), BATCH_RUNNING)

        res = self.sch.cancel_task('F(a=1)')
        self.assertEqual(res, {'task_id': 'F(a=1)', 'accepted': False, 'status': BATCH_RUNNING})
        self.assertEqual(task_status(self.sch, 'F(a=1)'), BATCH_RUNNING)

    def test_confirm_cancel(self):
        add_capable_worker(self.sch)
        token = start_task(self.sch, 'A')['task_token']
        self.sch.cancel_task('A')
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)
        # execution ownership released
        self.assertIsNone(self.sch._state.get_task('A').worker_running)

    def test_confirm_cancel_from_other_worker_ignored(self):
        add_capable_worker(self.sch)
        token = start_task(self.sch, 'A')['task_token']
        self.sch.cancel_task('A')
        self.sch.add_task(task_id='A', worker='someone-else', status=CANCELLED,
                          runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)

    def test_confirm_cancel_with_wrong_token_ignored(self):
        add_capable_worker(self.sch)
        start_task(self.sch, 'A')
        self.sch.cancel_task('A')
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED,
                          runnable=None, task_token='not-the-token')
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)

    def test_duplicate_confirm_ignored(self):
        add_capable_worker(self.sch)
        token = start_task(self.sch, 'A')['task_token']
        self.sch.cancel_task('A')
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)
        # a duplicate/late confirmation must not break anything
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)
        # and a repeated cancel_task reports the current status
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': CANCELLED})

    def test_reports_during_cancel_do_not_flip_state(self):
        add_capable_worker(self.sch)
        token = start_task(self.sch, 'A')['task_token']
        self.sch.cancel_task('A')

        # completion of the cancelled execution arrives after the cancel was
        # accepted: must not flip the task back
        self.sch.add_task(task_id='A', worker=WORKER, status=DONE, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)

        # failure of the cancelled execution: no FAILED state, no retry scheduled
        self.sch.add_task(task_id='A', worker=WORKER, status=FAILED, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)
        task = self.sch._state.get_task('A')
        self.assertIsNone(task.retry)
        self.assertEqual(task.num_failures(), 0)

        # dynamic dependencies of the cancelled execution are dropped
        self.sch.add_task(task_id='A', worker=WORKER, status=PENDING, runnable=None,
                          new_deps=['dyn-dep'], task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)
        self.assertNotIn('dyn-dep', task.deps)

        # progress reports must not flip the state either
        self.sch.set_task_progress_percentage('A', 50)
        self.sch.set_task_status_message('A', 'halfway')
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)

        # the cancellation still completes normally
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)

        # late reports of the cancelled execution stay ignored
        self.sch.add_task(task_id='A', worker=WORKER, status=DONE, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)

    def test_done_first_then_cancel_rejected(self):
        add_capable_worker(self.sch)
        token = start_task(self.sch, 'A')['task_token']
        # the DONE report is accepted before the cancel request
        self.sch.add_task(task_id='A', worker=WORKER, status=DONE, runnable=None, task_token=token)
        self.assertEqual(task_status(self.sch, 'A'), DONE)
        res = self.sch.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': DONE})

    def test_resources_held_until_cancel_confirmed(self):
        sch = make_scheduler(self._tmp.name, resources={'r': 1})
        add_capable_worker(sch)
        start_task(sch, 'A', resources={'r': 1})
        sch.add_task(task_id='B', worker=WORKER, resources={'r': 1})

        sch.cancel_task('A')
        # while CANCEL_REQUESTED the resource is still reserved
        self.assertIsNone(sch.get_work(worker=WORKER)['task_id'])
        self.assertEqual(sch.resources()['r']['used'], 1)

        token = sch._state.get_task('A').cancel_token
        sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token)
        self.assertEqual(sch.resources()['r']['used'], 0)
        # now the other task may use the resource
        self.assertEqual(sch.get_work(worker=WORKER)['task_id'], 'B')

    def test_downstream_of_cancelled_task_not_scheduled(self):
        add_capable_worker(self.sch)
        self.sch.add_task(task_id='A', worker=WORKER)
        self.sch.add_task(task_id='B', worker=WORKER, deps=['A'])
        self.sch.cancel_task('A')
        # B's dependency is not DONE, so B must not run
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])
        self.assertEqual(task_status(self.sch, 'B'), PENDING)

    def test_re_enable_cancelled_task(self):
        add_capable_worker(self.sch)
        token1 = start_task(self.sch, 'A')['task_token']
        self.sch.cancel_task('A')
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token1)
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)

        serialized = self.sch.re_enable_task('A')
        self.assertEqual(serialized['status'], PENDING)
        self.assertEqual(task_status(self.sch, 'A'), PENDING)

        # re-dispatch gets a fresh execution token
        token2 = self.sch.get_work(worker=WORKER)['task_token']
        self.assertTrue(token2)
        self.assertNotEqual(token1, token2)

        # stale messages of the old execution cannot stop the new one
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token1)
        self.assertEqual(task_status(self.sch, 'A'), RUNNING)
        self.sch.add_task(task_id='A', worker=WORKER, status=DONE, runnable=None, task_token=token1)
        self.assertEqual(task_status(self.sch, 'A'), RUNNING)
        # the new execution completes normally
        self.sch.add_task(task_id='A', worker=WORKER, status=DONE, runnable=None, task_token=token2)
        self.assertEqual(task_status(self.sch, 'A'), DONE)

    def test_re_enable_not_possible_while_cancel_requested(self):
        add_capable_worker(self.sch)
        start_task(self.sch, 'A')
        self.sch.cancel_task('A')
        self.assertEqual(self.sch.re_enable_task('A'), {})
        self.assertEqual(task_status(self.sch, 'A'), CANCEL_REQUESTED)

    def test_cancel_state_survives_dump_and_load(self):
        state_path = os.path.join(self._tmp.name, 'state.pickle')
        sch = Scheduler(state_path=state_path, resources={'r': 1})
        add_capable_worker(sch)
        token = start_task(sch, 'A', resources={'r': 1})['task_token']
        res = sch.cancel_task('A')
        self.assertTrue(res['accepted'])

        # recreate the scheduler from the persisted state
        sch2 = Scheduler(state_path=state_path, resources={'r': 1})
        sch2.load()
        task = sch2._state.get_task('A')
        self.assertEqual(task.status, CANCEL_REQUESTED)
        self.assertEqual(task.cancel_worker, WORKER)
        self.assertEqual(task.cancel_token, token)
        self.assertEqual(task.worker_running, WORKER)

        # resource is still reserved after the reload
        sch2.add_task(task_id='B', worker=WORKER, resources={'r': 1})
        self.assertIsNone(sch2.get_work(worker=WORKER)['task_id'])

        # the worker's next heartbeat re-delivers the cancel decision
        messages = sch2.ping(worker=WORKER)['rpc_messages']
        cancels = [m for m in messages if m['name'] == 'cancel_task']
        self.assertEqual(cancels, [{'name': 'cancel_task',
                                    'kwargs': {'task_id': 'A', 'task_token': token}}])

        # and the confirmation completes the cancellation
        sch2.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token)
        self.assertEqual(task_status(sch2, 'A'), CANCELLED)
        self.assertEqual(sch2.get_work(worker=WORKER)['task_id'], 'B')

    def test_state_file_without_cancel_fields_loads(self):
        state_path = os.path.join(self._tmp.name, 'state.pickle')
        sch = Scheduler(state_path=state_path)
        add_capable_worker(sch)
        start_task(sch, 'A')
        # simulate a state file written by an older luigi version
        for task in sch._state.get_active_tasks():
            for attr in ('running_token', 'cancel_token', 'cancel_worker', 'cancel_requested_at'):
                delattr(task, attr)
        sch.dump()

        sch2 = Scheduler(state_path=state_path)
        sch2.load()
        self.assertEqual(task_status(sch2, 'A'), RUNNING)
        # cancelling an old (token-less) execution still works
        res = sch2.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': True, 'status': CANCEL_REQUESTED})
        sch2.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=None)
        self.assertEqual(task_status(sch2, 'A'), CANCELLED)

    def test_cancel_requested_kept_when_worker_disappears(self):
        sch = make_scheduler(self._tmp.name, worker_disconnect_delay=0.0, resources={'r': 1})
        add_capable_worker(sch)
        start_task(sch, 'A', resources={'r': 1})
        sch.cancel_task('A')

        sch.prune()  # worker times out and is pruned

        # the task is neither failed nor removed and keeps its resource
        self.assertEqual(task_status(sch, 'A'), CANCEL_REQUESTED)
        self.assertEqual(sch.resources()['r']['used'], 1)
        sch.add_task(task_id='B', worker='other-worker', resources={'r': 1})
        self.assertIsNone(sch.get_work(worker='other-worker')['task_id'])

    def test_cancel_delivered_via_ping_and_get_work(self):
        add_capable_worker(self.sch)
        token = start_task(self.sch, 'A')['task_token']
        self.sch.cancel_task('A')

        # get_work carries the pending cancellation
        reply = self.sch.get_work(worker=WORKER)
        self.assertEqual(reply['cancel_tasks'], [{'task_id': 'A', 'task_token': token}])

        # ping carries it too, repeatedly (not a one-shot message)
        for _ in range(2):
            messages = self.sch.ping(worker=WORKER)['rpc_messages']
            cancels = [m for m in messages if m['name'] == 'cancel_task']
            self.assertEqual(cancels, [{'name': 'cancel_task',
                                        'kwargs': {'task_id': 'A', 'task_token': token}}])

        # another worker is not bothered
        self.assertNotIn('cancel_tasks', self.sch.get_work(worker='other-worker'))
        messages = self.sch.ping(worker='other-worker')['rpc_messages']
        self.assertEqual([m for m in messages if m['name'] == 'cancel_task'], [])

        # once confirmed, the notifications stop
        self.sch.add_task(task_id='A', worker=WORKER, status=CANCELLED, runnable=None, task_token=token)
        self.assertNotIn('cancel_tasks', self.sch.get_work(worker=WORKER))
        messages = self.sch.ping(worker=WORKER)['rpc_messages']
        self.assertEqual([m for m in messages if m['name'] == 'cancel_task'], [])

    def test_cancel_does_not_cascade(self):
        add_capable_worker(self.sch)
        # U is a shared upstream of A and B; S is an independent sibling
        self.sch.add_task(task_id='U', worker=WORKER)
        self.sch.add_task(task_id='A', worker=WORKER, deps=['U'])
        self.sch.add_task(task_id='B', worker=WORKER, deps=['U', 'A'])
        self.sch.add_task(task_id='S', worker=WORKER)

        self.sch.cancel_task('A')
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)
        # everything else is unaffected and schedulable
        self.assertEqual(task_status(self.sch, 'U'), PENDING)
        self.assertEqual(task_status(self.sch, 'S'), PENDING)
        self.assertEqual(task_status(self.sch, 'B'), PENDING)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'U')
        self.sch.add_task(task_id='U', worker=WORKER, status=DONE, runnable=None)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'S')
        # B stays blocked on the cancelled A even with U done
        self.assertIsNone(self.sch.get_work(worker=WORKER)['task_id'])

    def test_rescheduling_attempt_does_not_reopen_cancelled_task(self):
        self.sch.add_task(task_id='A', worker=WORKER)
        self.sch.cancel_task('A')
        # a worker (re)scheduling the task must not undo the cancellation,
        # but the bookkeeping is refreshed for a later explicit re-enable
        self.sch.add_task(task_id='A', worker=WORKER, status=PENDING, runnable=True)
        self.assertEqual(task_status(self.sch, 'A'), CANCELLED)
        self.sch.re_enable_task('A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')

    def test_cancel_task_is_public_rpc(self):
        self.assertIn('cancel_task', RPC_METHODS)
        self.assertTrue(hasattr(luigi.rpc.RemoteScheduler, 'cancel_task'))


class CancelTaskServerTest(server_test.ServerTestBase):
    """The cancel_task RPC works through the REST server / RemoteScheduler."""

    def get_app(self):
        self.scheduler = Scheduler()
        return luigi.server.app(self.scheduler)

    def test_cancel_task_over_http(self):
        remote = luigi.rpc.RemoteScheduler('http://localhost:%d' % self.get_http_port())

        res = remote.cancel_task('unknown-task')
        self.assertEqual(res, {'task_id': 'unknown-task', 'accepted': False, 'status': None})

        remote.add_worker('W', [('cancel_support', True)])
        remote.add_task(task_id='A', worker='W')
        reply = remote.get_work(worker='W', host='testhost')
        self.assertEqual(reply['task_id'], 'A')

        res = remote.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': True, 'status': CANCEL_REQUESTED})

        res = remote.cancel_task('A')
        self.assertEqual(res, {'task_id': 'A', 'accepted': False, 'status': CANCEL_REQUESTED})

        remote.add_task(task_id='A', worker='W', status=CANCELLED, runnable=None,
                        task_token=reply['task_token'])
        self.assertEqual(remote.cancel_task('A'),
                         {'task_id': 'A', 'accepted': False, 'status': CANCELLED})


class CancelTaskWorkerTest(LuigiTestCase):
    """Worker-side cancellation with real task processes."""

    def setUp(self):
        super(CancelTaskWorkerTest, self).setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.sch = make_scheduler(self._tmp.name)

    def _path(self, name):
        return os.path.join(self._tmp.name, name)

    def _controlled_task(self, name, cls=_FileControlledTask):
        return cls(started_file=self._path(name + '.started'),
                   release_file=self._path(name + '.release'),
                   done_file=self._path(name + '.done'))

    def _start_worker(self, worker):
        thread = _WorkerThread(worker)
        thread.start()
        self.addCleanup(lambda: thread.join(timeout=30))
        return thread

    def test_cancel_running_task_multiprocess(self):
        """
        One long-running task is cancelled and its process reaped while a
        sibling task completes normally; resources are only reused after the
        cancellation is confirmed.
        """
        worker = Worker(scheduler=self.sch, worker_id='W1', worker_processes=2,
                        ping_interval=0.05, wait_interval=0.05, wait_jitter=0,
                        keep_alive=True, cancel_timeout=1.5)
        self.sch._resources = {'cancel_test_resource': 1}

        task_a = self._controlled_task('a', cls=_TermResistantTask)
        task_b = _QuickTask(done_file=self._path('b.done'))
        task_c = _ResourceTask(done_file=self._path('c.done'))

        with worker:
            worker.add(task_a)
            worker.add(task_b)
            # C needs the resource A is going to hold; it is scheduled up
            # front so no scheduling RPC races with the running worker
            worker.add(task_c)
            thread = self._start_worker(worker)

            # wait until A actually runs in its own process (SIGTERM ignored there)
            self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None),
                            'task A never started')
            pid_a = read_pid(task_a.started_file)

            # the sibling task B completes while A is running
            self.assertTrue(wait_for(lambda: task_status(self.sch, task_b.task_id) == DONE),
                            'sibling task B did not complete')

            # C cannot start: A holds the only resource unit
            self.assertEqual(task_status(self.sch, task_c.task_id), PENDING)

            res = self.sch.cancel_task(task_a.task_id)
            self.assertEqual(res, {'task_id': task_a.task_id, 'accepted': True,
                                   'status': CANCEL_REQUESTED})

            # while the cancellation is unconfirmed (A ignores SIGTERM and the
            # kill only comes after cancel_timeout), C must not get A's resource
            last_active = self.sch._state.get_worker('W1').last_active
            deadline = time.time() + 1.0
            while time.time() < deadline:
                self.assertEqual(task_status(self.sch, task_a.task_id), CANCEL_REQUESTED)
                self.assertEqual(task_status(self.sch, task_c.task_id), PENDING)
                time.sleep(0.05)
            # the worker kept heartbeating while waiting for A's process to die
            self.assertGreater(self.sch._state.get_worker('W1').last_active, last_active)

            # A is killed after cancel_timeout, reaped and confirmed CANCELLED
            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == CANCELLED),
                            'task A was not confirmed cancelled')
            self.assertFalse(pid_alive(pid_a), 'process of cancelled task is still alive')

            # only now may C use the resource and complete
            self.assertTrue(wait_for(lambda: task_status(self.sch, task_c.task_id) == DONE),
                            'task C did not complete after the resource was released')

            thread.join(timeout=30)
            self.assertFalse(thread.is_alive(), 'worker did not shut down')
            self.assertIsNone(thread.error)
            self.assertFalse(worker.run_succeeded)

    def test_cancel_with_force_multiprocessing(self):
        """Cancellation also works with worker_processes=1 + force_multiprocessing."""
        worker = Worker(scheduler=self.sch, worker_id='W1', worker_processes=1,
                        force_multiprocessing=True, ping_interval=0.05,
                        wait_interval=0.05, wait_jitter=0, cancel_timeout=2.0)
        task_a = self._controlled_task('a')

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)

            self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))
            pid_a = read_pid(task_a.started_file)

            res = self.sch.cancel_task(task_a.task_id)
            self.assertTrue(res['accepted'])
            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == CANCELLED))
            self.assertFalse(pid_alive(pid_a))

            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)

    def test_cancel_before_process_started(self):
        """A cancel arriving between dispatch and process start stops the dispatch."""
        sch = self.sch

        class PauseableWorker(Worker):
            def __init__(self, *args, **kwargs):
                super(PauseableWorker, self).__init__(*args, **kwargs)
                self.about_to_run = threading.Event()
                self.may_run = threading.Event()

            def _run_task(self, task_id):
                self.about_to_run.set()
                self.may_run.wait(timeout=30)
                super(PauseableWorker, self)._run_task(task_id)

        worker = PauseableWorker(scheduler=sch, worker_id='W1', force_multiprocessing=True,
                                 ping_interval=0.05, wait_interval=0.05, wait_jitter=0)
        task_a = self._controlled_task('a')

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)

            # the worker holds the dispatch but has not started the process yet
            self.assertTrue(worker.about_to_run.wait(timeout=15))
            self.assertEqual(task_status(sch, task_a.task_id), RUNNING)

            res = sch.cancel_task(task_a.task_id)
            self.assertTrue(res['accepted'])

            # wait until the cancel notification arrived at the worker
            self.assertTrue(wait_for(lambda: task_a.task_id in worker._cancel_requests))
            worker.may_run.set()

            self.assertTrue(wait_for(lambda: task_status(sch, task_a.task_id) == CANCELLED))
            # the process was never started
            self.assertNotIn(task_a.task_id, worker._running_tasks)
            self.assertFalse(os.path.exists(task_a.started_file))

            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)

    def test_cancel_timeout_escalates_to_sigkill(self):
        """A task ignoring SIGTERM is killed within cancel_timeout and reaped."""
        worker = Worker(scheduler=self.sch, worker_id='W1', force_multiprocessing=True,
                        ping_interval=0.05, wait_interval=0.05, wait_jitter=0,
                        cancel_timeout=0.5)
        task_a = self._controlled_task('a', cls=_TermResistantTask)

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)

            self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))
            pid_a = read_pid(task_a.started_file)

            t0 = time.time()
            self.assertTrue(self.sch.cancel_task(task_a.task_id)['accepted'])
            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == CANCELLED),
                            'SIGTERM-resistant task was not killed and confirmed')
            elapsed = time.time() - t0
            # the kill only comes after the cancel_timeout grace period ...
            self.assertGreaterEqual(elapsed, 0.5)
            # ... but well within timeout plus scheduling slack
            self.assertLess(elapsed, 0.5 + 10.0)
            self.assertFalse(pid_alive(pid_a))

            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)

    def test_stale_result_queue_item_ignored(self):
        """A result queued by a previous execution must not affect the current one."""
        worker = Worker(scheduler=self.sch, worker_id='W1', force_multiprocessing=True,
                        ping_interval=0.05, wait_interval=0.05, wait_jitter=0)
        task_a = self._controlled_task('a')

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)
            try:
                self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))
                token = worker._task_execution_tokens[task_a.task_id]
                self.assertTrue(token)

                # a result from some previous execution of the same task id
                worker._task_result_queue.put(
                    (task_a.task_id, DONE, '', [], [], 'stale-execution-token'))

                # give the worker plenty of loop iterations to (not) process it
                time.sleep(1.0)
                self.assertEqual(task_status(self.sch, task_a.task_id), RUNNING)
                self.assertIn(task_a.task_id, worker._running_tasks)
                self.assertEqual(worker._task_execution_tokens[task_a.task_id], token)
            finally:
                # let the real execution finish
                open(task_a.release_file, 'w').close()

            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == DONE))
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)
            self.assertTrue(worker.run_succeeded)

    def test_redispatch_isolated_from_old_execution(self):
        """
        After re_enable_task the task runs again; cancel messages and stop
        reports of the old execution do not affect the new one.
        """
        worker = Worker(scheduler=self.sch, worker_id='W1', force_multiprocessing=True,
                        ping_interval=0.05, wait_interval=0.05, wait_jitter=0,
                        keep_alive=True)
        task_a = self._controlled_task('a')

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)
            try:
                # first execution is cancelled
                self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))
                pid1 = read_pid(task_a.started_file)
                token1 = self.sch._state.get_task(task_a.task_id).running_token
                self.assertTrue(self.sch.cancel_task(task_a.task_id)['accepted'])
                self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == CANCELLED))
                self.assertFalse(pid_alive(pid1))

                # re-open and let the worker pick the task up again
                self.sch.re_enable_task(task_a.task_id)
                self.assertTrue(wait_for(
                    lambda: task_status(self.sch, task_a.task_id) == RUNNING
                    and self.sch._state.get_task(task_a.task_id).running_token != token1))
                token2 = self.sch._state.get_task(task_a.task_id).running_token
                self.assertTrue(wait_for(
                    lambda: read_pid(task_a.started_file) not in (None, pid1)))
                pid2 = read_pid(task_a.started_file)

                # a stale cancel message for the old execution is discarded
                worker._note_cancel_request(task_a.task_id, token1)
                # a stale stop report for the old execution is ignored
                self.sch.add_task(task_id=task_a.task_id, worker='W1', status=CANCELLED,
                                  runnable=None, task_token=token1)
                time.sleep(1.0)
                self.assertEqual(task_status(self.sch, task_a.task_id), RUNNING)
                self.assertTrue(pid_alive(pid2))
                self.assertEqual(worker._task_execution_tokens[task_a.task_id], token2)
            finally:
                open(task_a.release_file, 'w').close()

            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == DONE))
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)

    def test_duplicate_cancel_notifications_idempotent(self):
        worker = Worker(scheduler=self.sch, worker_id='W1', force_multiprocessing=True,
                        ping_interval=0.05, wait_interval=0.05, wait_jitter=0)
        task_a = self._controlled_task('a')

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)

            self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))

            self.assertTrue(self.sch.cancel_task(task_a.task_id)['accepted'])
            token = self.sch._state.get_task(task_a.task_id).cancel_token

            # the same notification arriving multiple times is collapsed
            worker._note_cancel_request(task_a.task_id, token)
            worker._note_cancel_request(task_a.task_id, token)
            self.assertEqual(len(worker._cancel_requests), 1)

            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == CANCELLED))
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)

    def test_cancel_rejected_for_main_process_task(self):
        """Tasks executed in the worker's own process cannot be cancelled."""
        worker = Worker(scheduler=self.sch, worker_id='W1', worker_processes=1,
                        ping_interval=0.05, wait_interval=0.05, wait_jitter=0)
        task_a = self._controlled_task('a')

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)
            try:
                self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))
                self.assertEqual(task_status(self.sch, task_a.task_id), RUNNING)

                res = self.sch.cancel_task(task_a.task_id)
                self.assertEqual(res, {'task_id': task_a.task_id, 'accepted': False,
                                       'status': RUNNING})
                self.assertEqual(task_status(self.sch, task_a.task_id), RUNNING)
            finally:
                open(task_a.release_file, 'w').close()

            # the task completes normally and the worker was not killed
            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == DONE))
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)
            self.assertTrue(worker.run_succeeded)

    def test_result_queued_before_cancel_is_dropped(self):
        """
        The task finished and queued its DONE result, but the cancel is
        accepted before the worker reports it: the cancel wins.
        """
        worker = Worker(scheduler=self.sch, worker_id='W1', force_multiprocessing=True,
                        wait_interval=0.05, wait_jitter=0)
        task_a = self._controlled_task('a')

        with worker:
            worker._keep_alive_thread.stop()
            worker._keep_alive_thread.join()

            worker.add(task_a)
            worker._add_worker()
            # dispatch and run the task to completion, but do not let the
            # worker's main loop report the result yet
            self.assertEqual(worker._get_work().task_id, task_a.task_id)
            worker._run_task(task_a.task_id)
            open(task_a.release_file, 'w').close()
            task_process = worker._running_tasks[task_a.task_id]
            self.assertTrue(wait_for(lambda: not task_process.is_alive()))
            # the DONE result is sitting in the result queue now

            # the cancel is accepted before the DONE report reaches the scheduler
            self.assertTrue(self.sch.cancel_task(task_a.task_id)['accepted'])
            token = self.sch._state.get_task(task_a.task_id).cancel_token
            worker._note_cancel_request(task_a.task_id, token)

            worker._handle_next_task()

            self.assertEqual(task_status(self.sch, task_a.task_id), CANCELLED)
            self.assertNotIn(task_a.task_id, worker._running_tasks)

    def test_cancel_delivered_via_get_work(self):
        """The get_work RPC alone (no ping) also delivers cancellations."""
        worker = Worker(scheduler=self.sch, worker_id='W1', force_multiprocessing=True,
                        wait_interval=0.05, wait_jitter=0)
        task_a = self._controlled_task('a')

        with worker:
            # stop the keep-alive thread: the ping channel must not deliver
            worker._keep_alive_thread.stop()
            worker._keep_alive_thread.join()

            worker.add(task_a)
            worker._add_worker()
            self.assertEqual(worker._get_work().task_id, task_a.task_id)
            worker._run_task(task_a.task_id)
            self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))
            pid_a = read_pid(task_a.started_file)

            self.assertTrue(self.sch.cancel_task(task_a.task_id)['accepted'])
            # the next get_work carries the cancel decision
            worker._get_work()
            self.assertIn(task_a.task_id, worker._cancel_requests)

            # drive the worker's main-loop step until it confirms
            deadline = time.time() + 15
            while task_status(self.sch, task_a.task_id) != CANCELLED:
                self.assertLess(time.time(), deadline)
                worker._handle_next_task()
            self.assertFalse(pid_alive(pid_a))

    def test_cancel_rejected_when_task_runs_in_main_process(self):
        """
        A worker-level timeout makes the worker advertise cancel_support, but
        an explicit worker_timeout=0 on the task forces main-process
        execution: the worker rejects the cancel and the task runs through.
        """
        worker = Worker(scheduler=self.sch, worker_id='W1', worker_processes=1,
                        timeout=60, ping_interval=0.05, wait_interval=0.05, wait_jitter=0)
        task_a = self._controlled_task('a', cls=_MainProcessTask)

        with worker:
            worker.add(task_a)
            thread = self._start_worker(worker)
            try:
                self.assertTrue(wait_for(lambda: read_pid(task_a.started_file) is not None))
                self.assertEqual(task_status(self.sch, task_a.task_id), RUNNING)

                # the worker-level timeout makes the scheduler accept the cancel
                res = self.sch.cancel_task(task_a.task_id)
                self.assertEqual(res, {'task_id': task_a.task_id, 'accepted': True,
                                       'status': CANCEL_REQUESTED})
                # wait until the cancel notification actually arrived; the
                # worker's main loop is still blocked inside task.run()
                self.assertTrue(wait_for(lambda: task_a.task_id in worker._cancel_requests))
            finally:
                open(task_a.release_file, 'w').close()

            # the worker cannot kill its own main process: it rejects the
            # cancel, the task goes back to RUNNING and completes normally
            self.assertTrue(wait_for(lambda: task_status(self.sch, task_a.task_id) == DONE),
                            'cancel of a main-process task was not rejected')
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(thread.error)
            self.assertTrue(worker.run_succeeded)
            self.assertEqual(self.sch.cancel_task(task_a.task_id),
                             {'task_id': task_a.task_id, 'accepted': False, 'status': DONE})

    def test_cancel_timeout_config_validation(self):
        self.assertRaises(AssertionError, Worker, scheduler=self.sch, cancel_timeout=0)
        self.assertRaises(AssertionError, Worker, scheduler=self.sch, cancel_timeout=-1.0)
        self.assertRaises(AssertionError, Worker, scheduler=self.sch,
                          cancel_timeout=float('inf'))
        self.assertRaises(AssertionError, Worker, scheduler=self.sch,
                          cancel_timeout=float('nan'))
        # default is 5.0 seconds
        self.assertEqual(Worker(scheduler=self.sch)._config.cancel_timeout, 5.0)
