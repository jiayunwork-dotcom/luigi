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
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
Regression tests for the lifecycle of *batch executions*.

Two properties are exercised here:

* a batch that is dispatched, registered and finished must finish **all** its
  members and release its resources, no matter whether the runner's task id
  equals one of the members' ids (``batch_method=max``) or is independent
  (``batch_method=','.join``), and no matter whether the result is DONE or
  FAILED/abnormal exit;
* every registration/progress/result message belongs to exactly the
  execution that produced it. Re-dispatching the same member combination (a
  retry, or a re-submission after the inputs changed) creates a new
  execution; late messages of the previous one must not finish, reschedule or
  otherwise take over the new one.

Ordering is always controlled explicitly (a fixed sequence of scheduler
messages, a file barrier in the task process, or a barrier before the run),
never with sleeps as the only synchronisation.
"""

import multiprocessing
import os
import pickle
import signal
import tempfile
import threading
import time
import unittest

import luigi
import luigi.rpc
import luigi.server
from luigi.scheduler import (
    BATCH_EXECUTION_TOKEN_PREFIX, Scheduler, SimpleTaskState,
)
from luigi.task_status import (
    BATCH_RUNNING, DONE, FAILED, PENDING, RUNNING,
)
from luigi.worker import Worker

WORKER = 'batch-worker'


def make_scheduler(tmpdir, **kwargs):
    kwargs.setdefault('state_path', os.path.join(tmpdir, 'state.pickle'))
    kwargs.setdefault('retry_delay', 1000)
    kwargs.setdefault('stable_done_cooldown_secs', 0)
    return Scheduler(**kwargs)


def used_resources(sch):
    """The used amount of the ``slot`` resource, locally or over RPC."""
    if isinstance(sch, luigi.rpc.RemoteScheduler):
        return {entry['name']: entry['num_used']
                for entry in sch.resource_list()}['slot']
    return sch.resources()['slot']['used']


def status_of(sch, task_id):
    task = sch._state.get_task(task_id)
    return task.status if task is not None else None


def statuses(sch, task_ids):
    if isinstance(sch, luigi.rpc.RemoteScheduler):
        listing = sch.task_list('', '')
        return {task_id: listing[task_id]['status'] for task_id in task_ids}
    return {task_id: status_of(sch, task_id) for task_id in task_ids}


def add_member(sch, task_id, family='A', param='a', value=None, **kwargs):
    sch.add_task(
        worker=WORKER, task_id=task_id, family=family,
        params={param: str(value if value is not None else task_id)},
        batchable=True, **kwargs)


def register_runner(sch, runner_id, dispatch, family='A', params=None,
                    status=RUNNING, worker=WORKER, **kwargs):
    sch.add_task(
        worker=worker, task_id=runner_id, family=family,
        params=params if params is not None else dispatch['task_params'],
        status=status, batch_id=dispatch['batch_id'],
        task_token=dispatch['task_token'], **kwargs)


def report_runner(sch, runner_id, dispatch, status=DONE, expl='',
                  worker=WORKER, **kwargs):
    sch.add_task(
        worker=worker, task_id=runner_id, status=status, expl=expl,
        runnable=None, task_token=dispatch['task_token'], **kwargs)


def wait_for(condition, timeout=20.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


class BatchExecutionOrderingTest(unittest.TestCase):
    """Controlled ordering of dispatch / registration / result messages."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def _scheduler(self, **kwargs):
        return make_scheduler(self._tmp.name, **kwargs)

    def _dispatch_max_batch(self, sch, priorities=(0, -1, -2)):
        sch.add_task_batcher(worker=WORKER, task_family='A', batched_args=['a'])
        for value, priority in zip(range(len(priorities)), priorities):
            add_member(sch, 'A_%d' % value, value=value, priority=priority,
                       resources={'slot': 1})
        return sch.get_work(worker=WORKER, host='testhost')

    def test_max_runner_completes_every_member_and_releases_slot(self):
        """
        The runner is member A_2 itself (batch_method=max of 0, 1, 2). Its
        completion must finish the whole batch, not only itself.
        """
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)

        self.assertIsNone(dispatch['task_id'])
        self.assertEqual(['A_0', 'A_1', 'A_2'], dispatch['batch_task_ids'])
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING,
                          'A_2': BATCH_RUNNING}, statuses(sch, dispatch['batch_task_ids']))
        self.assertEqual(1, sch.resources()['slot']['used'])

        register_runner(sch, 'A_2', dispatch)
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING,
                          'A_2': RUNNING}, statuses(sch, dispatch['batch_task_ids']))

        report_runner(sch, 'A_2', dispatch, status=DONE)

        self.assertEqual({'A_0': DONE, 'A_1': DONE, 'A_2': DONE},
                         statuses(sch, dispatch['batch_task_ids']))
        self.assertEqual(0, sch.resources()['slot']['used'])
        self.assertEqual(set(), set(sch.task_list(RUNNING, '')))
        self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))

    def test_synthetic_runner_completes_every_member_and_releases_slot(self):
        """The runner has an id independent from every member (','.join)."""
        sch = self._scheduler(resources={'slot': 1})
        sch.add_task_batcher(worker=WORKER, task_family='V', batched_args=['v'])
        for value in ('1', '2'):
            sch.add_task(worker=WORKER, task_id='V_%s' % value, family='V',
                         params={'v': value}, batchable=True,
                         resources={'slot': 1})
        dispatch = sch.get_work(worker=WORKER, host='testhost')
        self.assertNotIn(dispatch['task_params']['v'], ('1', '2'))
        register_runner(sch, 'V_1_2', dispatch, family='V')

        self.assertEqual({'V_1': BATCH_RUNNING, 'V_2': BATCH_RUNNING},
                         statuses(sch, ('V_1', 'V_2')))
        self.assertEqual(RUNNING, status_of(sch, 'V_1_2'))

        report_runner(sch, 'V_1_2', dispatch, status=DONE)
        self.assertEqual({'V_1': DONE, 'V_2': DONE, 'V_1_2': DONE},
                         statuses(sch, ('V_1', 'V_2', 'V_1_2')))
        self.assertEqual(0, sch.resources()['slot']['used'])

    def test_max_runner_failure_fails_every_member_and_allows_retry(self):
        sch = self._scheduler(resources={'slot': 1}, retry_delay=0)
        dispatch = self._dispatch_max_batch(sch)
        register_runner(sch, 'A_2', dispatch)
        report_runner(sch, 'A_2', dispatch, status=FAILED, expl='"boom"')

        self.assertEqual({'A_0': FAILED, 'A_1': FAILED, 'A_2': FAILED},
                         statuses(sch, dispatch['batch_task_ids']))
        for task_id in dispatch['batch_task_ids']:
            self.assertEqual('"boom"', sch.fetch_error(task_id)['error'])
        self.assertEqual(0, sch.resources()['slot']['used'])
        self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))

        # The failure follows the normal retry rules: the tasks are FAILED
        # (not stuck BATCH_RUNNING) and become schedulable again after the
        # retry delay.
        sch.prune()
        self.assertEqual({'A_0': PENDING, 'A_1': PENDING, 'A_2': PENDING},
                         statuses(sch, dispatch['batch_task_ids']))
        retry = sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual(set(dispatch['batch_task_ids']),
                         set(retry['batch_task_ids']))
        self.assertNotEqual(dispatch['task_token'], retry['task_token'])

    def test_abnormal_exit_is_reported_as_batch_failure(self):
        """A child process dying is reported as FAILED and fails the batch."""
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)
        register_runner(sch, 'A_2', dispatch)
        # What Worker._purge_children() sends when the process dies.
        report_runner(sch, 'A_2', dispatch, status=FAILED,
                      expl='"Task A_2 died unexpectedly with exit code 1"')
        self.assertEqual({'A_0', 'A_1', 'A_2'},
                         set(sch.task_list(FAILED, '')))
        self.assertEqual(0, sch.resources()['slot']['used'])

    def test_partial_overlap_executions_are_separate(self):
        """A new execution may share some but not all members with an old one."""
        sch = self._scheduler(resources={'slot': 1})
        sch.add_task_batcher(worker=WORKER, task_family='A', batched_args=['a'])
        for value in (0, 1):
            add_member(sch, 'A_%d' % value, value=value, resources={'slot': 1})
        first = sch.get_work(worker=WORKER, host='testhost')
        register_runner(sch, 'A_1', first)
        report_runner(sch, 'A_1', first, status=DONE)
        self.assertEqual({'A_0': DONE, 'A_1': DONE}, statuses(sch, ('A_0', 'A_1')))

        # A_0 is re-submitted together with the new member A_2.
        add_member(sch, 'A_0', value=0, resources={'slot': 1})
        add_member(sch, 'A_2', value=2, resources={'slot': 1})
        second = sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual({'A_0', 'A_2'}, set(second['batch_task_ids']))
        self.assertNotEqual(first['task_token'], second['task_token'])

        # The old execution's result can not finish the new one.
        report_runner(sch, 'A_1', first, status=DONE)
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_2': BATCH_RUNNING},
                         statuses(sch, ('A_0', 'A_2')))
        self.assertEqual(1, sch.resources()['slot']['used'])

    def test_identical_combination_redispatch_is_a_new_execution(self):
        """The same combination and therefore batch_id, but a new execution."""
        sch = self._scheduler(resources={'slot': 1})
        sch.add_task_batcher(worker=WORKER, task_family='A', batched_args=['a'])
        for value in (0, 1):
            add_member(sch, 'A_%d' % value, value=value, resources={'slot': 1})
        first = sch.get_work(worker=WORKER, host='testhost')
        register_runner(sch, 'A_1', first)
        report_runner(sch, 'A_1', first, status=DONE)

        for value in (0, 1):
            add_member(sch, 'A_%d' % value, value=value, resources={'slot': 1})
        second = sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual(first['batch_id'], second['batch_id'])
        self.assertNotEqual(first['task_token'], second['task_token'])

        # Late registration and late result of the first round.
        sch.add_task(worker=WORKER, task_id='OLD_RUNNER', family='A',
                     params={'a': '0,1'}, status=RUNNING,
                     batch_id=first['batch_id'], task_token=first['task_token'])
        report_runner(sch, 'A_1', first, status=DONE)
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING},
                         statuses(sch, ('A_0', 'A_1')))
        self.assertEqual(1, sch.resources()['slot']['used'])
        self.assertIsNone(sch._state.get_task('OLD_RUNNER').worker_running)

        # The new execution still completes normally.
        register_runner(sch, 'A_1', second)
        report_runner(sch, 'A_1', second, status=DONE)
        self.assertEqual({'A_0': DONE, 'A_1': DONE}, statuses(sch, ('A_0', 'A_1')))
        self.assertEqual(0, sch.resources()['slot']['used'])

    def test_stale_failure_does_not_fail_new_execution(self):
        sch = self._scheduler(resources={'slot': 1})
        sch.add_task_batcher(worker=WORKER, task_family='A', batched_args=['a'])
        add_member(sch, 'A_0', value=0, resources={'slot': 1})
        add_member(sch, 'A_1', value=1, resources={'slot': 1})
        first = sch.get_work(worker=WORKER, host='testhost')
        register_runner(sch, 'A_1', first)
        report_runner(sch, 'A_1', first, status=DONE)

        add_member(sch, 'A_0', value=0, resources={'slot': 1})
        add_member(sch, 'A_1', value=1, resources={'slot': 1})
        second = sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual({'A_0', 'A_1'}, set(second['batch_task_ids']))

        report_runner(sch, 'A_1', first, status=FAILED, expl='"old failure"')
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING},
                         statuses(sch, ('A_0', 'A_1')))
        self.assertEqual(0, sch._state.get_task('A_1').num_failures())
        self.assertEqual(1, sch.resources()['slot']['used'])

    def test_duplicate_result_has_no_second_effect(self):
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)
        register_runner(sch, 'A_2', dispatch)
        report_runner(sch, 'A_2', dispatch, status=DONE)
        failures_before = {
            task_id: len(sch._state.get_task(task_id).failures)
            for task_id in dispatch['batch_task_ids']}
        done_before = statuses(sch, dispatch['batch_task_ids'])

        # Re-deliver the same result several times, both DONE and FAILED.
        for _ in range(3):
            report_runner(sch, 'A_2', dispatch, status=DONE)
            report_runner(sch, 'A_2', dispatch, status=FAILED, expl='"late"')

        self.assertEqual(done_before, statuses(sch, dispatch['batch_task_ids']))
        self.assertEqual(failures_before, {
            task_id: len(sch._state.get_task(task_id).failures)
            for task_id in dispatch['batch_task_ids']})
        self.assertEqual(0, sch.resources()['slot']['used'])

    def test_other_worker_cannot_register_or_finish_batch(self):
        sch = self._scheduler(resources={'slot': 1})
        sch.add_worker('intruder', [])
        dispatch = self._dispatch_max_batch(sch)
        # A different worker presents the correct token: it is not the owner.
        register_runner(sch, 'A_2', dispatch, worker='intruder')
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING,
                          'A_2': BATCH_RUNNING},
                         statuses(sch, dispatch['batch_task_ids']))
        # A_2 belongs to the dispatching worker; the intruder did not take it
        # over. (Worker._add_task stores the dispatching worker because the
        # get_work reset named it.)
        self.assertNotEqual('intruder', sch._state.get_task('A_2').worker_running)

        report_runner(sch, 'A_2', dispatch, status=DONE, worker='intruder')
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING,
                          'A_2': BATCH_RUNNING},
                         statuses(sch, dispatch['batch_task_ids']))
        self.assertEqual(1, sch.resources()['slot']['used'])

    def test_resend_batch_runner_gets_fresh_execution(self):
        """
        A worker that lost track of the runner re-asks for work
        (``current_tasks=()``). The batch is re-dispatched with a new token;
        the old execution's result must not finish it.
        """
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)
        register_runner(sch, 'A_2', dispatch)

        resent = sch.get_work(worker=WORKER, host='testhost', current_tasks=())
        self.assertEqual('A_2', resent['task_id'])
        self.assertNotEqual(dispatch['task_token'], resent['task_token'])
        self.assertEqual(set(dispatch['batch_task_ids']),
                         set(resent['batch_task_ids']))
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING,
                          'A_2': RUNNING},
                         statuses(sch, dispatch['batch_task_ids']))
        # the batch still holds exactly one slot, not two
        self.assertEqual(1, sch.resources()['slot']['used'])

        report_runner(sch, 'A_2', dispatch, status=DONE)
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING,
                          'A_2': RUNNING},
                         statuses(sch, dispatch['batch_task_ids']))

        report_runner(sch, 'A_2', resent, status=DONE)
        self.assertEqual({'A_0': DONE, 'A_1': DONE, 'A_2': DONE},
                         statuses(sch, dispatch['batch_task_ids']))
        self.assertEqual(0, sch.resources()['slot']['used'])

    def test_runner_pending_reschedules_whole_batch(self):
        """Dynamic requirements from the runner release all members."""
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)
        register_runner(sch, 'A_2', dispatch)
        sch.add_task(worker=WORKER, task_id='A_2', status=PENDING, runnable=None,
                     task_token=dispatch['task_token'])
        self.assertEqual({'A_0': PENDING, 'A_1': PENDING, 'A_2': PENDING},
                         statuses(sch, dispatch['batch_task_ids']))
        self.assertEqual(0, sch.resources()['slot']['used'])
        # A late DONE of the old execution must not finish the new round.
        report_runner(sch, 'A_2', dispatch, status=DONE)
        self.assertEqual({'A_0': PENDING, 'A_1': PENDING, 'A_2': PENDING},
                         statuses(sch, dispatch['batch_task_ids']))

    def test_independent_batch_does_not_release_other_batch_resource(self):
        """Finishing one batch must not free the resources of another."""
        sch = self._scheduler(resources={'slot': 1})
        sch.add_task_batcher(worker=WORKER, task_family='A', batched_args=['a'])
        sch.add_task_batcher(worker=WORKER, task_family='B', batched_args=['a'])
        add_member(sch, 'A_0', family='A', value=0, resources={'slot': 1})
        add_member(sch, 'A_1', family='A', value=1, resources={'slot': 1})
        add_member(sch, 'B_0', family='B', value=0, resources={'slot': 1})
        add_member(sch, 'B_1', family='B', value=1, resources={'slot': 1})

        first = sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual('A', first['task_family'])
        register_runner(sch, 'A_1', first, family='A')
        # While A holds the only slot, B can not be dispatched.
        self.assertIsNone(sch.get_work(worker=WORKER, host='testhost')['task_id'])
        report_runner(sch, 'A_1', first, status=DONE)

        second = sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual('B', second['task_family'])
        register_runner(sch, 'B_1', second, family='B')
        self.assertEqual(1, sch.resources()['slot']['used'])
        report_runner(sch, 'B_1', second, status=DONE)
        self.assertEqual(0, sch.resources()['slot']['used'])

    def test_unrelated_tasks_and_heartbeat_keep_working(self):
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)
        register_runner(sch, 'A_2', dispatch)

        sch.add_task(worker=WORKER, task_id='UNRELATED')
        sch.ping(worker=WORKER)
        sch.add_task(worker=WORKER, task_id='UNRELATED', status=DONE, runnable=None)
        self.assertEqual(DONE, status_of(sch, 'UNRELATED'))
        # the batch is unaffected and still finishes
        report_runner(sch, 'A_2', dispatch, status=DONE)
        self.assertEqual({'A_0': DONE, 'A_1': DONE, 'A_2': DONE},
                         statuses(sch, dispatch['batch_task_ids']))

    def test_dump_and_load_preserves_current_execution(self):
        state_path = os.path.join(self._tmp.name, 'state.pickle')
        sch = Scheduler(state_path=state_path, resources={'slot': 1},
                        stable_done_cooldown_secs=0)
        sch.add_task_batcher(worker=WORKER, task_family='A', batched_args=['a'])
        for value in (0, 1):
            add_member(sch, 'A_%d' % value, value=value, resources={'slot': 1})
        old = sch.get_work(worker=WORKER, host='testhost')
        register_runner(sch, 'A_1', old)
        sch.dump()

        sch2 = Scheduler(state_path=state_path, resources={'slot': 1},
                         stable_done_cooldown_secs=0)
        sch2.load()
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': RUNNING},
                         statuses(sch2, ('A_0', 'A_1')))
        self.assertEqual(1, sch2.resources()['slot']['used'])

        # The reloaded scheduler still accepts the current execution's result.
        report_runner(sch2, 'A_1', old, status=DONE)
        self.assertEqual({'A_0': DONE, 'A_1': DONE}, statuses(sch2, ('A_0', 'A_1')))
        self.assertEqual(0, sch2.resources()['slot']['used'])

        # ... and still rejects the old execution after a new dispatch.
        for value in (0, 1):
            add_member(sch2, 'A_%d' % value, value=value, resources={'slot': 1})
        new = sch2.get_work(worker=WORKER, host='testhost')
        self.assertNotEqual(old['task_token'], new['task_token'])
        sch2.dump()
        sch3 = Scheduler(state_path=state_path, resources={'slot': 1},
                         stable_done_cooldown_secs=0)
        sch3.load()
        report_runner(sch3, 'A_1', old, status=DONE)
        self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING},
                         statuses(sch3, ('A_0', 'A_1')))
        # the reloaded new execution still finishes
        register_runner(sch3, 'A_1', new)
        report_runner(sch3, 'A_1', new, status=DONE)
        self.assertEqual({'A_0': DONE, 'A_1': DONE}, statuses(sch3, ('A_0', 'A_1')))
        self.assertEqual(0, sch3.resources()['slot']['used'])

    def test_stale_result_does_not_create_placeholder_task(self):
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)
        register_runner(sch, 'A_2', dispatch)
        report_runner(sch, 'A_2', dispatch, status=DONE)

        # Re-deliver a registration and a result of the finished execution for
        # task ids that are not known any more: no placeholder is created for
        # the stale result.
        report_runner(sch, 'SOME_OLD_RUNNER', dispatch, status=DONE)
        self.assertFalse(sch._state.has_task('SOME_OLD_RUNNER'))
        self.assertEqual({'A_0': DONE, 'A_1': DONE, 'A_2': DONE},
                         statuses(sch, dispatch['batch_task_ids']))

    def test_batch_tokens_are_prefixed(self):
        sch = self._scheduler(resources={'slot': 1})
        dispatch = self._dispatch_max_batch(sch)
        self.assertTrue(dispatch['task_token'].startswith(
            BATCH_EXECUTION_TOKEN_PREFIX))


class BatchExecutionStateCompatibilityTest(unittest.TestCase):

    def test_old_state_file_without_batch_fields_loads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'state.pickle')
            task = luigi.scheduler.Task(task_id='A_1', status=PENDING, deps=None)
            for attr in ('batch_execution', 'batch_runner', 'batchable'):
                delattr(task, attr)
            with open(path, 'wb') as f:
                pickle.dump(({'A_1': task},
                             {'w': luigi.scheduler.Worker('w')}, {}), f)
            state = SimpleTaskState(path)
            state.load()
            self.assertIsNone(state.get_task('A_1').batch_execution)
            self.assertIsNone(state.get_task('A_1').batch_runner)
            # The task is still schedulable after the load.
            self.assertEqual(PENDING, state.get_task('A_1').status)

    def test_legacy_state_without_batch_executions_mapping(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'state.pickle')
            with open(path, 'wb') as f:
                pickle.dump(({}, {'w': luigi.scheduler.Worker('w')}, {}), f)
            state = SimpleTaskState(path)
            state.load()
            self.assertEqual({}, state._batch_executions)

    def test_legacy_state_batch_still_finishes(self):
        """
        A state file written before executions were tracked (no fourth state
        element, no per-task execution fields) can be loaded and the batch it
        left running still finishes as a group when the runner reports.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'state.pickle')
            sch = Scheduler(state_path=path, resources={'slot': 1},
                            stable_done_cooldown_secs=0)
            sch.add_task_batcher(worker=WORKER, task_family='A',
                                 batched_args=['a'])
            for value in (0, 1):
                add_member(sch, 'A_%d' % value, value=value,
                           resources={'slot': 1})
            dispatch = sch.get_work(worker=WORKER, host='testhost')
            register_runner(sch, 'A_1_2', dispatch)
            # Rewrite the persisted state as an older version would have.
            for task in sch._state.get_active_tasks():
                task.batch_execution = None
                task.batch_runner = None
            with open(path, 'wb') as f:
                pickle.dump(sch._state.get_state()[:3], f)

            sch2 = Scheduler(state_path=path, resources={'slot': 1},
                             stable_done_cooldown_secs=0)
            sch2.load()
            self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': BATCH_RUNNING,
                              'A_1_2': RUNNING},
                             statuses(sch2, ('A_0', 'A_1', 'A_1_2')))
            # Older clients only know batch_id, which the legacy path handles.
            sch2.add_task(worker=WORKER, task_id='A_1_2', status=DONE,
                          runnable=None)
            self.assertEqual({'A_0': DONE, 'A_1': DONE, 'A_1_2': DONE},
                             statuses(sch2, ('A_0', 'A_1', 'A_1_2')))
            self.assertEqual(0, sch2.resources()['slot']['used'])


class _ServerProcess:
    """A real luigi server in a separate process, reached over real HTTP."""

    def __init__(self):
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('127.0.0.1', 0))
        self.port = sock.getsockname()[1]
        sock.close()
        self.process = None

    def __enter__(self):
        self.process = multiprocessing.Process(
            target=luigi.server.run,
            kwargs={'api_port': self.port, 'address': '127.0.0.1'})
        self.process.start()

        def reachable():
            import urllib.request
            try:
                urllib.request.urlopen(
                    'http://127.0.0.1:%d/api/task_list?data=%s'
                    % (self.port, '%7B%7D'), timeout=1).read()
                return True
            except Exception:
                return False

        if not wait_for(reachable, timeout=30):
            self.__exit__(None, None, None)
            raise RuntimeError('server did not start')
        return luigi.rpc.RemoteScheduler('http://127.0.0.1:%d' % self.port)

    def __exit__(self, *exc):
        if self.process is not None:
            self.process.terminate()
            self.process.join(timeout=10)
            if self.process.is_alive():
                os.kill(self.process.pid, signal.SIGKILL)
                self.process.join(timeout=5)
        return False


class BatchExecutionRpcTest(unittest.TestCase):
    """The same guarantees through the real REST RPC (RemoteScheduler)."""

    def test_batch_execution_over_http(self):
        with _ServerProcess() as remote:
            remote.add_worker(WORKER, [('cancel_support', True)])
            remote.add_task_batcher(worker=WORKER, task_family='A',
                                    batched_args=['a'])
            remote.update_resource('slot', 1)
            for value in (0, 1):
                remote.add_task(worker=WORKER, task_id='A_%d' % value,
                                family='A', params={'a': str(value)},
                                batchable=True, resources={'slot': 1})
            dispatch = remote.get_work(worker=WORKER, host='testhost')
            self.assertIsNone(dispatch['task_id'])
            self.assertTrue(dispatch['task_token'].startswith(
                BATCH_EXECUTION_TOKEN_PREFIX))

            remote.add_task(worker=WORKER, task_id='A_1', family='A',
                            params={'a': '1'}, status=RUNNING,
                            batch_id=dispatch['batch_id'],
                            task_token=dispatch['task_token'])
            self.assertEqual({'A_0': BATCH_RUNNING, 'A_1': RUNNING},
                             statuses(remote, ('A_0', 'A_1')))

            remote.add_task(worker=WORKER, task_id='A_1', status=DONE,
                            runnable=None, task_token=dispatch['task_token'])
            self.assertEqual({'A_0': DONE, 'A_1': DONE},
                             statuses(remote, ('A_0', 'A_1')))
            self.assertEqual(0, used_resources(remote))

            # A duplicate over the wire has no second effect.
            remote.add_task(worker=WORKER, task_id='A_1', status=DONE,
                            runnable=None, task_token=dispatch['task_token'])
            self.assertEqual({'A_0': DONE, 'A_1': DONE},
                             statuses(remote, ('A_0', 'A_1')))


class _MaxBatchTask(luigi.Task):
    """max() combines the values; running the max covers every lower value."""
    value = luigi.IntParameter(batch_method=max)
    control_dir = luigi.Parameter()

    def _path(self, value):
        return os.path.join(self.control_dir, 'max-%d.ran' % value)

    def run(self):
        with open(self._path(self.value), 'w') as f:
            f.write(str(self.value))

    def complete(self):
        # Running the max covers all lower members of the batch.
        return any(os.path.exists(self._path(ran))
                   for ran in range(self.value, self.value + 1000))


class _CsvBatchTask(luigi.Task):
    """','.join combines the values; the runner id is independent."""
    value = luigi.parameter.Parameter(batch_method=','.join)
    control_dir = luigi.Parameter()

    def _path(self, value):
        return os.path.join(self.control_dir, 'csv-%s.ran' % value)

    def run(self):
        for value in self.value.split(','):
            with open(self._path(value), 'w') as f:
                f.write(value)

    def complete(self):
        return all(os.path.exists(self._path(value))
                   for value in self.value.split(','))


class _DynamicBatchTask(luigi.Task):
    """A batch runner that first yields a dynamic (non-batch) dependency."""
    value = luigi.parameter.Parameter(batch_method=','.join)
    control_dir = luigi.Parameter()
    resources = {'slot': 1}

    def _path(self, name):
        return os.path.join(self.control_dir, name)

    def run(self):
        yield _DynamicDependency(control_dir=self.control_dir)
        for value in self.value.split(','):
            with open(self._path('dyn-%s.ran' % value), 'w') as f:
                f.write(value)

    def complete(self):
        return all(os.path.exists(self._path('dyn-%s.ran' % value))
                   for value in self.value.split(','))


class _DynamicDependency(luigi.Task):
    control_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.control_dir, 'dyn.dep'))

    def run(self):
        with open(self.output().path, 'w') as f:
            f.write('dep')

    def complete(self):
        return self.output().exists()


class BatchExecutionWorkerTest(unittest.TestCase):
    """Real multi-process worker runs."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def _worker(self, name):
        sch = make_scheduler(self._tmp.name, resources={'slot': 1})
        worker = Worker(scheduler=sch, worker_id=name, worker_processes=2,
                        wait_interval=0.02, wait_jitter=0)
        return sch, worker

    def test_dynamic_requirements_keep_members_schedulable(self):
        """
        When the batch runner yields new requirements, the members go back to
        the pool (never stuck BATCH_RUNNING) and the batch runs again after
        the dependency is complete.
        """
        sch, worker = self._worker('W-dyn')
        tasks = [_DynamicBatchTask(str(i), control_dir=self._tmp.name)
                 for i in range(2)]
        for task in tasks:
            self.assertTrue(worker.add(task))
        with worker:
            self.assertTrue(worker.run())

        for task in tasks:
            self.assertTrue(task.complete(), task)
            self.assertEqual(DONE, status_of(sch, task.task_id), task)
        self.assertEqual(0, used_resources(sch))
        self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))
        self.assertEqual(set(), set(sch.task_list(RUNNING, '')))

    def test_max_batch_worker_finishes_all_members(self):
        sch, worker = self._worker('W-max')
        # max() picks 2, so the runner id equals member max-2's id.
        tasks = [_MaxBatchTask(i, control_dir=self._tmp.name) for i in range(3)]
        for task in tasks:
            self.assertTrue(worker.add(task))
        with worker:
            self.assertTrue(worker.run())

        for task in tasks:
            self.assertTrue(task.complete(), task)
            self.assertEqual(DONE, status_of(sch, task.task_id), task)
        self.assertEqual(0, sch.resources()['slot']['used'])
        self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))
        self.assertEqual(set(), set(sch.task_list(RUNNING, '')))

    def test_csv_batch_worker_finishes_all_members(self):
        sch, worker = self._worker('W-csv')
        tasks = [_CsvBatchTask(str(i), control_dir=self._tmp.name) for i in range(3)]
        for task in tasks:
            self.assertTrue(worker.add(task))
        with worker:
            self.assertTrue(worker.run())

        for task in tasks:
            self.assertTrue(task.complete(), task)
            self.assertEqual(DONE, status_of(sch, task.task_id), task)
        self.assertEqual(0, sch.resources()['slot']['used'])
        self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))

    def test_batch_worker_failure_marks_all_members_failed(self):
        class FailingMaxTask(luigi.Task):
            value = luigi.IntParameter(batch_method=max)

            def run(self):
                raise RuntimeError('batch exploded %s' % self.value)

            def complete(self):
                return False

        sch, worker = self._worker('W-fail')
        tasks = [FailingMaxTask(i) for i in range(2)]
        for task in tasks:
            self.assertTrue(worker.add(task))
        with worker:
            self.assertFalse(worker.run())

        for task in tasks:
            self.assertEqual(FAILED, status_of(sch, task.task_id), task)
            self.assertIsNotNone(sch.fetch_error(task.task_id)['error'])
        self.assertEqual(0, sch.resources()['slot']['used'])
        self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))

    def test_abnormally_exiting_child_fails_whole_batch(self):
        class CrashingMaxTask(luigi.Task):
            value = luigi.IntParameter(batch_method=max)

            def run(self):
                os.kill(os.getpid(), signal.SIGKILL)

            def complete(self):
                return False

        sch, worker = self._worker('W-crash')
        tasks = [CrashingMaxTask(i) for i in range(2)]
        for task in tasks:
            self.assertTrue(worker.add(task))
        with worker:
            self.assertFalse(worker.run())

        for task in tasks:
            self.assertEqual(FAILED, status_of(sch, task.task_id), task)
        self.assertEqual(0, sch.resources()['slot']['used'])
        self.assertEqual(set(), set(sch.task_list(BATCH_RUNNING, '')))

    def test_redispatch_after_resubmission_ignores_late_result(self):
        """
        The same members complete, are re-submitted and dispatched again while
        the previous round's result is still arriving. The late result must
        not finish the new round.
        """
        sch = make_scheduler(self._tmp.name, resources={'slot': 1})
        sch.add_task_batcher(worker=WORKER, task_family='R', batched_args=['a'])
        for value in (0, 1):
            sch.add_task(worker=WORKER, task_id='R_%d' % value, family='R',
                         params={'a': str(value)}, batchable=True,
                         resources={'slot': 1})
        first = sch.get_work(worker=WORKER, host='testhost')
        register_runner(sch, 'R_1', first, family='R')
        report_runner(sch, 'R_1', first, status=DONE)

        for value in (0, 1):
            sch.add_task(worker=WORKER, task_id='R_%d' % value, family='R',
                         params={'a': str(value)}, batchable=True,
                         resources={'slot': 1})
        second = sch.get_work(worker=WORKER, host='testhost')
        self.assertEqual(first['batch_id'], second['batch_id'])

        # Late registration/result of round one.
        register_runner(sch, 'R_OLD', first, family='R')
        report_runner(sch, 'R_1', first, status=DONE)
        self.assertEqual({'R_0': BATCH_RUNNING, 'R_1': BATCH_RUNNING},
                         statuses(sch, ('R_0', 'R_1')))

        register_runner(sch, 'R_1', second, family='R')
        report_runner(sch, 'R_1', second, status=DONE)
        self.assertEqual({'R_0': DONE, 'R_1': DONE}, statuses(sch, ('R_0', 'R_1')))
        self.assertEqual(0, sch.resources()['slot']['used'])


class _BarrierBatchTask(luigi.Task):
    """
    Batchable task whose batch runner waits for one release file per member.

    The runner's value is the comma-joined member values, so its task id is
    independent of every member id.
    """
    value = luigi.parameter.Parameter(batch_method=','.join)
    control_dir = luigi.Parameter()
    resources = {'slot': 1}

    def _started(self, value):
        return os.path.join(self.control_dir, '%s.started' % value)

    def _release(self, value):
        return os.path.join(self.control_dir, '%s.release' % value)

    def _ran(self, value):
        return os.path.join(self.control_dir, '%s.ran' % value)

    def run(self):
        for value in self.value.split(','):
            with open(self._started(value), 'w') as f:
                f.write(str(os.getpid()))
        for value in self.value.split(','):
            # Barrier: wait for the test to release every member.
            deadline = time.time() + 60
            while not os.path.exists(self._release(value)):
                if time.time() > deadline:
                    raise RuntimeError('never released %s' % value)
                time.sleep(0.01)
            with open(self._ran(value), 'w') as f:
                f.write(value)

    def complete(self):
        return all(os.path.exists(self._ran(value))
                   for value in self.value.split(','))


class BatchExecutionRealRpcWorkerTest(unittest.TestCase):
    """
    A real multi-process worker against a real HTTP scheduler.

    The worker talks to the scheduler exclusively through RemoteScheduler, so
    the batch dispatch, the runner registration and the result report all
    travel over the real RPC layer. The task process blocks on a file barrier
    until the test has observed the registered batch through the RPC, so the
    ordering does not depend on sleeps.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def test_csv_batch_worker_over_real_rpc(self):
        state = {}

        with _ServerProcess() as remote:
            remote.update_resource('slot', 1)
            worker = Worker(scheduler=remote, worker_id='W-rpc',
                            worker_processes=2, wait_interval=0.02,
                            wait_jitter=0)
            tasks = [_BarrierBatchTask(str(i), control_dir=self._tmp.name)
                     for i in range(2)]
            for task in tasks:
                self.assertTrue(worker.add(task))

            def run_worker():
                with worker:
                    state['ok'] = worker.run()

            thread = threading.Thread(target=run_worker)
            thread.start()
            try:
                # The test has not released anything yet: the runner must be
                # registered and registered as a separate task.
                runner_id = None
                for task in tasks:
                    self.assertTrue(wait_for(
                        lambda: os.path.exists(
                            os.path.join(self._tmp.name, '%s.started' % task.value)),
                        timeout=60))
                self.assertTrue(wait_for(
                    lambda: bool(remote.task_search('_BarrierBatchTask_')), timeout=60))
                search = remote.task_search('_BarrierBatchTask_')
                self.assertIn(RUNNING, search)
                runner_ids = list(search[RUNNING].keys())
                self.assertEqual(1, len(runner_ids), runner_ids)
                runner_id = runner_ids[0]
                self.assertNotIn(runner_id, [t.task_id for t in tasks])
                # Both members are still BATCH_RUNNING, waiting for the result.
                self.assertEqual({t.task_id for t in tasks},
                                 set(search.get(BATCH_RUNNING, {})))
                for task in tasks:
                    open(os.path.join(self._tmp.name, '%s.release' % task.value),
                         'w').close()
            finally:
                thread.join(timeout=120)

            self.assertFalse(thread.is_alive())
            self.assertTrue(state.get('ok'))

            # All members finished and the slot is free again.
            search = remote.task_search('_BarrierBatchTask_')
            done_ids = set(search.get(DONE, {}))
            for task in tasks:
                self.assertIn(task.task_id, done_ids)
                self.assertTrue(task.complete(), task)
            self.assertEqual({}, search.get(BATCH_RUNNING, {}))
            self.assertEqual({}, search.get(RUNNING, {}))
            self.assertEqual(0, used_resources(remote))


if __name__ == '__main__':
    unittest.main()
