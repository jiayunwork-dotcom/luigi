# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
The worker communicates with the scheduler and does two things:

1. Sends all tasks that has to be run
2. Gets tasks from the scheduler that should be run

When running in local mode, the worker talks directly to a :py:class:`~luigi.scheduler.Scheduler` instance.
When you run a central server, the worker will talk to the scheduler using a :py:class:`~luigi.rpc.RemoteScheduler` instance.

Everything in this module is private to luigi and may change in incompatible
ways between versions. The exception is the exception types and the
:py:class:`worker` config class.
"""

import collections
import collections.abc
import datetime
import getpass
import importlib
import logging
import math
import multiprocessing
import os
import signal
import subprocess
import sys
import contextlib
import functools

import queue as Queue
import random
import socket
import threading
import time
import traceback

from luigi import notifications
from luigi.event import Event
from luigi.task_register import load_task
from luigi.scheduler import DISABLED, DONE, FAILED, PENDING, UNKNOWN, Scheduler, RetryPolicy
from luigi.scheduler import WORKER_STATE_ACTIVE, WORKER_STATE_DISABLED
from luigi.target import Target
from luigi.task import Task, Config, DynamicRequirements, flatten
from luigi.task_register import TaskClassException
from luigi.task_status import RUNNING, CANCELLED
from luigi.parameter import BoolParameter, FloatParameter, IntParameter, OptionalParameter, Parameter, TimeDeltaParameter

import json

logger = logging.getLogger('luigi-interface')

# Prevent fork() from being called during a C-level getaddrinfo() which uses a process-global mutex,
# that may not be unlocked in child process, resulting in the process being locked indefinitely.
fork_lock = threading.Lock()

# Why we assert on _WAIT_INTERVAL_EPS:
# multiprocessing.Queue.get() is undefined for timeout=0 it seems:
# https://docs.python.org/3.4/library/multiprocessing.html#multiprocessing.Queue.get.
# I also tried with really low epsilon, but then ran into the same issue where
# the test case "test_external_dependency_worker_is_patient" got stuck. So I
# unscientifically just set the final value to a floating point number that
# "worked for me".
_WAIT_INTERVAL_EPS = 0.00001


def _is_external(task):
    return task.run is None or task.run == NotImplemented


def _get_retry_policy_dict(task):
    return RetryPolicy(task.retry_count, task.disable_hard_timeout, task.disable_window)._asdict()


class TaskException(Exception):
    pass


GetWorkResponse = collections.namedtuple('GetWorkResponse', (
    'task_id',
    'running_tasks',
    'n_pending_tasks',
    'n_unique_pending',
    'n_pending_last_scheduled',
    'worker_state',
))


class TaskProcess(multiprocessing.Process):

    """ Wrap all task execution in this class.

    Mainly for convenience since this is run in a separate process. """

    # mapping of status_reporter attributes to task attributes that are added to tasks
    # before they actually run, and removed afterwards
    forward_reporter_attributes = {
        "update_tracking_url": "set_tracking_url",
        "update_status_message": "set_status_message",
        "update_progress_percentage": "set_progress_percentage",
        "decrease_running_resources": "decrease_running_resources",
        "scheduler_messages": "scheduler_messages",
    }

    def __init__(self, task, worker_id, result_queue, status_reporter,
                 use_multiprocessing=False, worker_timeout=0, check_unfulfilled_deps=True,
                 check_complete_on_run=False, task_completion_cache=None, execution_token=None):
        super(TaskProcess, self).__init__()
        self.task = task
        self.worker_id = worker_id
        self.result_queue = result_queue
        self.status_reporter = status_reporter
        self.worker_timeout = task.worker_timeout if task.worker_timeout is not None else worker_timeout
        self.timeout_time = time.time() + self.worker_timeout if self.worker_timeout else None
        self.use_multiprocessing = use_multiprocessing or self.timeout_time is not None
        self.check_unfulfilled_deps = check_unfulfilled_deps
        self.check_complete_on_run = check_complete_on_run
        self.task_completion_cache = task_completion_cache
        # identifies the execution (dispatch) this process belongs to; results
        # are tagged with it so stale results of a previous execution of the
        # same task id can be discarded
        self.execution_token = execution_token

        # completeness check using the cache
        self.check_complete = functools.partial(check_complete_cached, completion_cache=task_completion_cache)

    def _run_get_new_deps(self):
        task_gen = self.task.run()

        if not isinstance(task_gen, collections.abc.Generator):
            return None

        next_send = None
        while True:
            try:
                if next_send is None:
                    requires = next(task_gen)
                else:
                    requires = task_gen.send(next_send)
            except StopIteration:
                return None

            # if requires is not a DynamicRequirements, create one to use its default behavior
            if not isinstance(requires, DynamicRequirements):
                requires = DynamicRequirements(requires)

            if not requires.complete(self.check_complete):
                # not all requirements are complete, return them which adds them to the tree
                new_deps = [(t.task_module, t.task_family, t.to_str_params())
                            for t in requires.flat_requirements]
                return new_deps

            # get the next generator result
            next_send = requires.paths

    def run(self):
        logger.info('[pid %s] Worker %s running   %s', os.getpid(), self.worker_id, self.task)

        if self.use_multiprocessing:
            # Need to have different random seeds if running in separate processes
            processID = os.getpid()
            currentTime = time.time()
            random.seed(processID * currentTime)

        status = FAILED
        expl = ''
        missing = []
        new_deps = []
        try:
            # Verify that all the tasks are fulfilled! For external tasks we
            # don't care about unfulfilled dependencies, because we are just
            # checking completeness of self.task so outputs of dependencies are
            # irrelevant.
            if self.check_unfulfilled_deps and not _is_external(self.task):
                missing = []
                for dep in self.task.deps():
                    if not self.check_complete(dep):
                        nonexistent_outputs = [output for output in flatten(dep.output()) if not output.exists()]
                        if nonexistent_outputs:
                            missing.append(f'{dep.task_id} ({", ".join(map(str, nonexistent_outputs))})')
                        else:
                            missing.append(dep.task_id)
                if missing:
                    deps = 'dependency' if len(missing) == 1 else 'dependencies'
                    raise RuntimeError('Unfulfilled %s at run time: %s' % (deps, ', '.join(missing)))
            self.task.trigger_event(Event.START, self.task)
            t0 = time.time()
            status = None

            if _is_external(self.task):
                # External task
                if self.check_complete(self.task):
                    status = DONE
                else:
                    status = FAILED
                    expl = 'Task is an external data dependency ' \
                        'and data does not exist (yet?).'
            else:
                with self._forward_attributes():
                    new_deps = self._run_get_new_deps()
                if not new_deps:
                    if not self.check_complete_on_run:
                        # update the cache
                        if self.task_completion_cache is not None:
                            self.task_completion_cache[self.task.task_id] = True
                        status = DONE
                    elif self.check_complete(self.task):
                        status = DONE
                    else:
                        raise TaskException("Task finished running, but complete() is still returning false.")
                else:
                    status = PENDING

            if new_deps:
                logger.info(
                    '[pid %s] Worker %s new requirements      %s',
                    os.getpid(), self.worker_id, self.task)
            elif status == DONE:
                self.task.trigger_event(
                    Event.PROCESSING_TIME, self.task, time.time() - t0)
                expl = self.task.on_success()
                logger.info('[pid %s] Worker %s done      %s', os.getpid(),
                            self.worker_id, self.task)
                self.task.trigger_event(Event.SUCCESS, self.task)

        except KeyboardInterrupt:
            raise
        except BaseException as ex:
            status = FAILED
            expl = self._handle_run_exception(ex)

        finally:
            self.result_queue.put(
                (self.task.task_id, status, expl, missing, new_deps, self.execution_token))

    def _handle_run_exception(self, ex):
        logger.exception("[pid %s] Worker %s failed    %s", os.getpid(), self.worker_id, self.task)
        self.task.trigger_event(Event.FAILURE, self.task, ex)
        return self.task.on_failure(ex)

    def _recursive_terminate(self):
        import psutil

        try:
            parent = psutil.Process(self.pid)
            children = parent.children(recursive=True)

            # terminate parent. Give it a chance to clean up
            super(TaskProcess, self).terminate()
            parent.wait()

            # terminate children
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    continue
        except psutil.NoSuchProcess:
            return

    def terminate(self):
        """Terminate this process and its subprocesses."""
        # default terminate() doesn't cleanup child processes, it orphans them.
        try:
            return self._recursive_terminate()
        except ImportError:
            return super(TaskProcess, self).terminate()

    @contextlib.contextmanager
    def _forward_attributes(self):
        # forward configured attributes to the task
        for reporter_attr, task_attr in self.forward_reporter_attributes.items():
            setattr(self.task, task_attr, getattr(self.status_reporter, reporter_attr))
        try:
            yield self
        finally:
            # reset attributes again
            for reporter_attr, task_attr in self.forward_reporter_attributes.items():
                setattr(self.task, task_attr, None)


# This code and the task_process_context config key currently feels a bit ad-hoc.
# Discussion on generalizing it into a plugin system: https://github.com/spotify/luigi/issues/1897
class ContextManagedTaskProcess(TaskProcess):
    def __init__(self, context, *args, **kwargs):
        super(ContextManagedTaskProcess, self).__init__(*args, **kwargs)
        self.context = context

    def run(self):
        if self.context:
            logger.debug('Importing module and instantiating ' + self.context)
            module_path, class_name = self.context.rsplit('.', 1)
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)

            with cls(self):
                super(ContextManagedTaskProcess, self).run()
        else:
            super(ContextManagedTaskProcess, self).run()


class TaskStatusReporter:
    """
    Reports task status information to the scheduler.

    This object must be pickle-able for passing to `TaskProcess` on systems
    where fork method needs to pickle the process object (e.g.  Windows).
    """
    def __init__(self, scheduler, task_id, worker_id, scheduler_messages):
        self._task_id = task_id
        self._worker_id = worker_id
        self._scheduler = scheduler
        self.scheduler_messages = scheduler_messages

    def update_tracking_url(self, tracking_url):
        self._scheduler.add_task(
            task_id=self._task_id,
            worker=self._worker_id,
            status=RUNNING,
            tracking_url=tracking_url
        )

    def update_status_message(self, message):
        self._scheduler.set_task_status_message(self._task_id, message)

    def update_progress_percentage(self, percentage):
        self._scheduler.set_task_progress_percentage(self._task_id, percentage)

    def decrease_running_resources(self, decrease_resources):
        self._scheduler.decrease_running_task_resources(self._task_id, decrease_resources)


class SchedulerMessage:
    """
    Message object that is build by the the :py:class:`Worker` when a message from the scheduler is
    received and passed to the message queue of a :py:class:`Task`.
    """

    def __init__(self, scheduler, task_id, message_id, content, **payload):
        super(SchedulerMessage, self).__init__()

        self._scheduler = scheduler
        self._task_id = task_id
        self._message_id = message_id

        self.content = content
        self.payload = payload

    def __str__(self):
        return str(self.content)

    def __eq__(self, other):
        return self.content == other

    def respond(self, response):
        self._scheduler.add_scheduler_message_response(self._task_id, self._message_id, response)


class SingleProcessPool:
    """
    Dummy process pool for using a single processor.

    Imitates the api of multiprocessing.Pool using single-processor equivalents.
    """

    def apply_async(self, function, args):
        return function(*args)

    def close(self):
        pass

    def join(self):
        pass


class DequeQueue(collections.deque):
    """
    deque wrapper implementing the Queue interface.
    """

    def put(self, obj, block=None, timeout=None):
        return self.append(obj)

    def get(self, block=None, timeout=None):
        try:
            return self.pop()
        except IndexError:
            raise Queue.Empty


class AsyncCompletionException(Exception):
    """
    Exception indicating that something went wrong with checking complete.
    """

    def __init__(self, trace):
        self.trace = trace


class TracebackWrapper:
    """
    Class to wrap tracebacks so we can know they're not just strings.
    """

    def __init__(self, trace):
        self.trace = trace


def check_complete_cached(task, completion_cache=None):
    # check if cached and complete
    cache_key = task.task_id
    if completion_cache is not None and completion_cache.get(cache_key):
        return True

    # (re-)check the status
    is_complete = task.complete()

    # tell the cache when complete
    if completion_cache is not None and is_complete:
        completion_cache[cache_key] = is_complete

    return is_complete


def check_complete(task, out_queue, completion_cache=None):
    """
    Checks if task is complete, puts the result to out_queue, optionally using the completion cache.
    """
    logger.debug("Checking if %s is complete", task)
    try:
        is_complete = check_complete_cached(task, completion_cache)
    except Exception:
        is_complete = TracebackWrapper(traceback.format_exc())
    out_queue.put((task, is_complete))


class worker(Config):
    # NOTE: `section.config-variable` in the config_path argument is deprecated in favor of `worker.config_variable`

    id = Parameter(default='',
                   description='Override the auto-generated worker_id')
    ping_interval = FloatParameter(default=1.0,
                                   config_path=dict(section='core', name='worker-ping-interval'))
    keep_alive = BoolParameter(default=False,
                               config_path=dict(section='core', name='worker-keep-alive'))
    count_uniques = BoolParameter(default=False,
                                  config_path=dict(section='core', name='worker-count-uniques'),
                                  description='worker-count-uniques means that we will keep a '
                                  'worker alive only if it has a unique pending task, as '
                                  'well as having keep-alive true')
    count_last_scheduled = BoolParameter(default=False,
                                         description='Keep a worker alive only if there are '
                                                     'pending tasks which it was the last to '
                                                     'schedule.')
    wait_interval = FloatParameter(default=1.0,
                                   config_path=dict(section='core', name='worker-wait-interval'))
    wait_jitter = FloatParameter(default=5.0)

    max_keep_alive_idle_duration = TimeDeltaParameter(default=datetime.timedelta(0))

    max_reschedules = IntParameter(default=1,
                                   config_path=dict(section='core', name='worker-max-reschedules'))
    timeout = IntParameter(default=0,
                           config_path=dict(section='core', name='worker-timeout'))
    task_limit = IntParameter(default=None,
                              config_path=dict(section='core', name='worker-task-limit'))
    retry_external_tasks = BoolParameter(default=False,
                                         config_path=dict(section='core', name='retry-external-tasks'),
                                         description='If true, incomplete external tasks will be '
                                         'retested for completion while Luigi is running.')
    send_failure_email = BoolParameter(default=True,
                                       description='If true, send e-mails directly from the worker'
                                                   'on failure')
    no_install_shutdown_handler = BoolParameter(default=False,
                                                description='If true, the SIGUSR1 shutdown handler will'
                                                'NOT be install on the worker')
    check_unfulfilled_deps = BoolParameter(default=True,
                                           description='If true, check for completeness of '
                                           'dependencies before running a task')
    check_complete_on_run = BoolParameter(default=False,
                                          description='If true, only mark tasks as done after running if they are complete. '
                                          'Regardless of this setting, the worker will always check if external '
                                          'tasks are complete before marking them as done.')
    force_multiprocessing = BoolParameter(default=False,
                                          description='If true, use multiprocessing also when '
                                          'running with 1 worker')
    task_process_context = OptionalParameter(default=None,
                                             description='If set to a fully qualified class name, the class will '
                                             'be instantiated with a TaskProcess as its constructor parameter and '
                                             'applied as a context manager around its run() call, so this can be '
                                             'used for obtaining high level customizable monitoring or logging of '
                                             'each individual Task run.')
    cache_task_completion = BoolParameter(default=False,
                                          description='If true, cache the response of successful completion checks '
                                          'of tasks assigned to a worker. This can especially speed up tasks with '
                                          'dynamic dependencies but assumes that the completion status does not change '
                                          'after it was true the first time.')
    cancel_timeout = FloatParameter(default=5.0,
                                    description='Grace period in seconds a cancelled task process is given to '
                                    'exit after being terminated, before it is killed forcibly. Must be a '
                                    'finite positive number.')


class KeepAliveThread(threading.Thread):
    """
    Periodically tell the scheduler that the worker still lives.
    """

    def __init__(self, scheduler, worker_id, ping_interval, rpc_message_callback):
        super(KeepAliveThread, self).__init__()
        self._should_stop = threading.Event()
        self._scheduler = scheduler
        self._worker_id = worker_id
        self._ping_interval = ping_interval
        self._rpc_message_callback = rpc_message_callback

    def stop(self):
        self._should_stop.set()

    def run(self):
        while True:
            self._should_stop.wait(self._ping_interval)
            if self._should_stop.is_set():
                logger.info("Worker %s was stopped. Shutting down Keep-Alive thread" % self._worker_id)
                break
            with fork_lock:
                response = None
                try:
                    response = self._scheduler.ping(worker=self._worker_id)
                except BaseException:  # httplib.BadStatusLine:
                    logger.warning('Failed pinging scheduler')

                # handle rpc messages
                if response:
                    for message in response["rpc_messages"]:
                        self._rpc_message_callback(message)


def rpc_message_callback(fn):
    fn.is_rpc_message_callback = True
    return fn


class Worker:
    """
    Worker object communicates with a scheduler.

    Simple class that talks to a scheduler and:

    * tells the scheduler what it has to do + its dependencies
    * asks for stuff to do (pulls it in a loop and runs it)
    """

    def __init__(self, scheduler=None, worker_id=None, worker_processes=1, assistant=False, **kwargs):
        if scheduler is None:
            scheduler = Scheduler()

        self.worker_processes = int(worker_processes)
        self._worker_info = self._generate_worker_info()

        self._config = worker(**kwargs)

        worker_id = worker_id or self._config.id or self._generate_worker_id(self._worker_info)

        assert self._config.wait_interval >= _WAIT_INTERVAL_EPS, "[worker] wait_interval must be positive"
        assert self._config.wait_jitter >= 0.0, "[worker] wait_jitter must be equal or greater than zero"
        assert (math.isfinite(self._config.cancel_timeout) and self._config.cancel_timeout > 0), \
            "[worker] cancel_timeout must be a finite positive number"

        self._id = worker_id
        self._scheduler = scheduler
        self._assistant = assistant
        self._stop_requesting_work = False

        self.host = socket.gethostname()
        self._scheduled_tasks = {}
        self._suspended_tasks = {}
        # Batch member tasks by the id of the task that runs them. The batch
        # runner may be an independent task id (e.g. ','.join) or the id of
        # one of the members itself; both are handled the same way.
        self._batch_running_tasks = {}
        self._batch_families_sent = set()

        self._first_task = None

        self.add_succeeded = True
        self.run_succeeded = True

        self.unfulfilled_counts = collections.defaultdict(int)

        # note that ``signal.signal(signal.SIGUSR1, fn)`` only works inside the main execution thread, which is why we
        # provide the ability to conditionally install the hook.
        if not self._config.no_install_shutdown_handler:
            try:
                signal.signal(signal.SIGUSR1, self.handle_interrupt)
                signal.siginterrupt(signal.SIGUSR1, False)
            except AttributeError:
                pass

        # Keep info about what tasks are running (could be in other processes)
        self._task_result_queue = multiprocessing.Queue()
        self._running_tasks = {}
        self._idle_since = None

        # Execution (dispatch) tokens of the tasks we are currently running,
        # as handed out by get_work. Used to tag results and to match cancel
        # requests to the exact execution they refer to.
        self._task_execution_tokens = {}

        # Pending cancellation requests, task_id -> dict with keys
        # 'token', 'term_sent', 'kill_sent', 'deadline'. Filled by the
        # scheduler's cancel notifications (via ping/get_work) and advanced
        # non-blockingly by the main worker loop in _process_cancels().
        self._cancel_requests = {}

        # mp-safe dictionary for caching completation checks across task processes
        self._task_completion_cache = None
        if self._config.cache_task_completion:
            self._task_completion_cache = multiprocessing.Manager().dict()

        # Stuff for execution_summary
        self._add_task_history = []
        self._get_work_response_history = []

    def _add_task(self, *args, **kwargs):
        """
        Call ``self._scheduler.add_task``, but store the values too so we can
        implement :py:func:`luigi.execution_summary.summary`.
        """
        task_id = kwargs['task_id']
        status = kwargs['status']
        runnable = kwargs['runnable']
        task = self._scheduled_tasks.get(task_id)
        if task:
            self._add_task_history.append((task, status, runnable))
            kwargs['owners'] = task._owner_list()

        batch_tasks = self._batch_running_tasks.pop(task_id, None)
        if batch_tasks is not None:
            # The runner's own history entry is recorded above; its members
            # share its outcome (a batch finishes as a whole).
            for batch_task in batch_tasks:
                if batch_task.task_id != task_id:
                    self._add_task_history.append((batch_task, status, True))

        if task and kwargs.get('params'):
            kwargs['param_visibilities'] = task._get_param_visibilities()

        self._scheduler.add_task(*args, **kwargs)

        logger.info('Informed scheduler that task   %s   has status   %s', task_id, status)

    def __enter__(self):
        """
        Start the KeepAliveThread.
        """
        self._keep_alive_thread = KeepAliveThread(self._scheduler, self._id,
                                                  self._config.ping_interval,
                                                  self._handle_rpc_message)
        self._keep_alive_thread.daemon = True
        self._keep_alive_thread.start()
        return self

    def __exit__(self, type, value, traceback):
        """
        Stop the KeepAliveThread and kill still running tasks.
        """
        self._keep_alive_thread.stop()
        self._keep_alive_thread.join()
        for task in self._running_tasks.values():
            if task.is_alive():
                task.terminate()
        self._task_result_queue.close()
        return False  # Don't suppress exception

    def _generate_worker_info(self):
        # Generate as much info as possible about the worker
        # Some of these calls might not be available on all OS's
        args = [('salt', '%09d' % random.randrange(0, 10_000_000_000)),
                ('workers', self.worker_processes)]
        try:
            args += [('host', socket.gethostname())]
        except BaseException:
            pass
        try:
            args += [('username', getpass.getuser())]
        except BaseException:
            pass
        try:
            args += [('pid', os.getpid())]
        except BaseException:
            pass
        try:
            sudo_user = os.getenv("SUDO_USER")
            if sudo_user:
                args.append(('sudo_user', sudo_user))
        except BaseException:
            pass
        return args

    def _generate_worker_id(self, worker_info):
        worker_info_str = ', '.join(['{}={}'.format(k, v) for k, v in worker_info])
        return 'Worker({})'.format(worker_info_str)

    def _validate_task(self, task):
        if not isinstance(task, Task):
            raise TaskException('Can not schedule non-task %s' % task)

        if not task.initialized():
            # we can't get the repr of it since it's not initialized...
            raise TaskException('Task of class %s not initialized. Did you override __init__ and forget to call super(...).__init__?' % task.__class__.__name__)

    def _log_complete_error(self, task, tb):
        log_msg = "Will not run {task} or any dependencies due to error in complete() method:\n{tb}".format(task=task, tb=tb)
        logger.warning(log_msg)

    def _log_dependency_error(self, task, tb):
        log_msg = "Will not run {task} or any dependencies due to error in deps() method:\n{tb}".format(task=task, tb=tb)
        logger.warning(log_msg)

    def _log_unexpected_error(self, task):
        logger.exception("Luigi unexpected framework error while scheduling %s", task)  # needs to be called from within except clause

    def _announce_scheduling_failure(self, task, expl):
        try:
            self._scheduler.announce_scheduling_failure(
                worker=self._id,
                task_name=str(task),
                family=task.task_family,
                params=task.to_str_params(only_significant=True),
                expl=expl,
                owners=task._owner_list(),
            )
        except Exception:
            formatted_traceback = traceback.format_exc()
            self._email_unexpected_error(task, formatted_traceback)
            raise

    def _email_complete_error(self, task, formatted_traceback):
        self._announce_scheduling_failure(task, formatted_traceback)
        if self._config.send_failure_email:
            self._email_error(task, formatted_traceback,
                              subject="Luigi: {task} failed scheduling. Host: {host}",
                              headline="Will not run {task} or any dependencies due to error in complete() method",
                              )

    def _email_dependency_error(self, task, formatted_traceback):
        self._announce_scheduling_failure(task, formatted_traceback)
        if self._config.send_failure_email:
            self._email_error(task, formatted_traceback,
                              subject="Luigi: {task} failed scheduling. Host: {host}",
                              headline="Will not run {task} or any dependencies due to error in deps() method",
                              )

    def _email_unexpected_error(self, task, formatted_traceback):
        # this sends even if failure e-mails are disabled, as they may indicate
        # a more severe failure that may not reach other alerting methods such
        # as scheduler batch notification
        self._email_error(task, formatted_traceback,
                          subject="Luigi: Framework error while scheduling {task}. Host: {host}",
                          headline="Luigi framework error",
                          )

    def _email_task_failure(self, task, formatted_traceback):
        if self._config.send_failure_email:
            self._email_error(task, formatted_traceback,
                              subject="Luigi: {task} FAILED. Host: {host}",
                              headline="A task failed when running. Most likely run() raised an exception.",
                              )

    def _email_error(self, task, formatted_traceback, subject, headline):
        formatted_subject = subject.format(task=task, host=self.host)
        formatted_headline = headline.format(task=task, host=self.host)
        command = subprocess.list2cmdline(sys.argv)
        message = notifications.format_task_error(
            formatted_headline, task, command, formatted_traceback)
        notifications.send_error_email(formatted_subject, message, task.owner_email)

    def _handle_task_load_error(self, exception, task_ids, batch_id=None,
                                task_token=None):
        msg = 'Cannot find task(s) sent by scheduler: {}'.format(','.join(task_ids))
        logger.exception(msg)
        subject = 'Luigi: {}'.format(msg)
        error_message = notifications.wrap_traceback(exception)
        extra = {}
        if batch_id is not None:
            extra['batch_id'] = batch_id
        if task_token is not None:
            extra['task_token'] = task_token
        for task_id in task_ids:
            self._add_task(
                worker=self._id,
                task_id=task_id,
                status=FAILED,
                runnable=False,
                expl=error_message,
                **extra
            )
        notifications.send_error_email(subject, error_message)

    def add(self, task, multiprocess=False, processes=0):
        """
        Add a Task for the worker to check and possibly schedule and run.

        Returns True if task and its dependencies were successfully scheduled or completed before.
        """
        if self._first_task is None and hasattr(task, 'task_id'):
            self._first_task = task.task_id
        self.add_succeeded = True
        if multiprocess:
            queue = multiprocessing.Manager().Queue()
            pool = multiprocessing.Pool(processes=processes if processes > 0 else None)
        else:
            queue = DequeQueue()
            pool = SingleProcessPool()
        self._validate_task(task)
        pool.apply_async(check_complete, [task, queue, self._task_completion_cache])

        # we track queue size ourselves because len(queue) won't work for multiprocessing
        queue_size = 1
        try:
            seen = {task.task_id}
            while queue_size:
                current = queue.get()
                queue_size -= 1
                item, is_complete = current
                for next in self._add(item, is_complete):
                    if next.task_id not in seen:
                        self._validate_task(next)
                        seen.add(next.task_id)
                        pool.apply_async(check_complete, [next, queue, self._task_completion_cache])
                        queue_size += 1
        except (KeyboardInterrupt, TaskException):
            raise
        except Exception as ex:
            self.add_succeeded = False
            formatted_traceback = traceback.format_exc()
            self._log_unexpected_error(task)
            task.trigger_event(Event.BROKEN_TASK, task, ex)
            self._email_unexpected_error(task, formatted_traceback)
            raise
        finally:
            pool.close()
            pool.join()
        return self.add_succeeded

    def _add_task_batcher(self, task):
        family = task.task_family
        if family not in self._batch_families_sent:
            task_class = type(task)
            batch_param_names = task_class.batch_param_names()
            if batch_param_names:
                self._scheduler.add_task_batcher(
                    worker=self._id,
                    task_family=family,
                    batched_args=batch_param_names,
                    max_batch_size=task.max_batch_size,
                )
            self._batch_families_sent.add(family)

    def _add(self, task, is_complete):
        if self._config.task_limit is not None and len(self._scheduled_tasks) >= self._config.task_limit:
            logger.warning('Will not run %s or any dependencies due to exceeded task-limit of %d', task, self._config.task_limit)
            deps = None
            status = UNKNOWN
            runnable = False

        else:
            formatted_traceback = None
            try:
                self._check_complete_value(is_complete)
            except KeyboardInterrupt:
                raise
            except AsyncCompletionException as ex:
                formatted_traceback = ex.trace
            except BaseException:
                formatted_traceback = traceback.format_exc()

            if formatted_traceback is not None:
                self.add_succeeded = False
                self._log_complete_error(task, formatted_traceback)
                task.trigger_event(Event.DEPENDENCY_MISSING, task)
                self._email_complete_error(task, formatted_traceback)
                deps = None
                status = UNKNOWN
                runnable = False

            elif is_complete:
                deps = None
                status = DONE
                runnable = False
                task.trigger_event(Event.DEPENDENCY_PRESENT, task)

            elif _is_external(task):
                deps = None
                status = PENDING
                runnable = self._config.retry_external_tasks
                task.trigger_event(Event.DEPENDENCY_MISSING, task)
                logger.warning('Data for %s does not exist (yet?). The task is an '
                               'external data dependency, so it cannot be run from'
                               ' this luigi process.', task)

            else:
                try:
                    deps = task.deps()
                    self._add_task_batcher(task)
                except Exception as ex:
                    formatted_traceback = traceback.format_exc()
                    self.add_succeeded = False
                    self._log_dependency_error(task, formatted_traceback)
                    task.trigger_event(Event.BROKEN_TASK, task, ex)
                    self._email_dependency_error(task, formatted_traceback)
                    deps = None
                    status = UNKNOWN
                    runnable = False
                else:
                    status = PENDING
                    runnable = True

            if task.disabled:
                status = DISABLED

            if deps:
                for d in deps:
                    self._validate_dependency(d)
                    task.trigger_event(Event.DEPENDENCY_DISCOVERED, task, d)
                    yield d  # return additional tasks to add

                deps = [d.task_id for d in deps]

        self._scheduled_tasks[task.task_id] = task
        self._add_task(
            worker=self._id,
            task_id=task.task_id,
            status=status,
            deps=deps,
            runnable=runnable,
            priority=task.priority,
            resources=task.process_resources(),
            params=task.to_str_params(),
            family=task.task_family,
            module=task.task_module,
            batchable=task.batchable,
            retry_policy_dict=_get_retry_policy_dict(task),
            accepts_messages=task.accepts_messages,
        )

    def _validate_dependency(self, dependency):
        if isinstance(dependency, Target):
            raise Exception('requires() can not return Target objects. Wrap it in an ExternalTask class')
        elif not isinstance(dependency, Task):
            raise Exception('requires() must return Task objects but {} is a {}'.format(dependency, type(dependency)))

    def _check_complete_value(self, is_complete):
        if is_complete not in (True, False):
            if isinstance(is_complete, TracebackWrapper):
                raise AsyncCompletionException(is_complete.trace)
            raise Exception("Return value of Task.complete() must be boolean (was %r)" % is_complete)

    def _supports_cancellation(self):
        """
        Whether this worker runs every task in its own process, so that the
        scheduler may ask it to cancel a single task execution.
        """
        return bool(self._config.force_multiprocessing
                    or self.worker_processes > 1
                    or self._config.timeout > 0)

    def _add_worker(self):
        self._worker_info.append(('first_task', self._first_task))
        self._worker_info.append(('cancel_support', self._supports_cancellation()))
        self._scheduler.add_worker(self._id, self._worker_info)

    def _log_remote_tasks(self, get_work_response):
        logger.debug("Done")
        logger.debug("There are no more tasks to run at this time")
        if get_work_response.running_tasks:
            for r in get_work_response.running_tasks:
                logger.debug('%s is currently run by worker %s', r['task_id'], r['worker'])
        elif get_work_response.n_pending_tasks:
            logger.debug(
                "There are %s pending tasks possibly being run by other workers",
                get_work_response.n_pending_tasks)
            if get_work_response.n_unique_pending:
                logger.debug(
                    "There are %i pending tasks unique to this worker",
                    get_work_response.n_unique_pending)
            if get_work_response.n_pending_last_scheduled:
                logger.debug(
                    "There are %i pending tasks last scheduled by this worker",
                    get_work_response.n_pending_last_scheduled)

    def _get_work_task_id(self, get_work_response):
        if get_work_response.get('task_id') is not None:
            return get_work_response['task_id']
        elif 'batch_id' in get_work_response:
            try:
                task = load_task(
                    module=get_work_response.get('task_module'),
                    task_name=get_work_response['task_family'],
                    params_str=get_work_response['task_params'],
                )
            except Exception as ex:
                self._handle_task_load_error(
                    ex, get_work_response['batch_task_ids'],
                    batch_id=get_work_response.get('batch_id'),
                    task_token=get_work_response.get('task_token'))
                self.run_succeeded = False
                return None

            self._scheduler.add_task(
                worker=self._id,
                task_id=task.task_id,
                module=get_work_response.get('task_module'),
                family=get_work_response['task_family'],
                params=task.to_str_params(),
                status=RUNNING,
                batch_id=get_work_response['batch_id'],
                # Prove that this registration belongs to the dispatch the
                # worker is answering; a registration presented with the
                # token of an earlier execution is rejected by the scheduler.
                task_token=get_work_response.get('task_token'),
            )
            return task.task_id
        else:
            return None

    def _get_work(self):
        if self._stop_requesting_work:
            return GetWorkResponse(None, 0, 0, 0, 0, WORKER_STATE_DISABLED)

        if self.worker_processes > 0:
            logger.debug("Asking scheduler for work...")
            r = self._scheduler.get_work(
                worker=self._id,
                host=self.host,
                assistant=self._assistant,
                current_tasks=list(self._running_tasks.keys()),
            )
        else:
            logger.debug("Checking if tasks are still pending")
            r = self._scheduler.count_pending(worker=self._id)

        running_tasks = r['running_tasks']
        task_id = self._get_work_task_id(r)

        # cancellations travel with the normal get_work RPC as well as with
        # pings; both channels are idempotent re-derivations of scheduler state
        for cancel in r.get('cancel_tasks') or ():
            self._note_cancel_request(cancel['task_id'], cancel.get('task_token'))

        self._get_work_response_history.append({
            'task_id': task_id,
            'running_tasks': running_tasks,
        })

        if task_id is not None and task_id not in self._scheduled_tasks:
            logger.info('Did not schedule %s, will load it dynamically', task_id)

            try:
                # TODO: we should obtain the module name from the server!
                self._scheduled_tasks[task_id] = \
                    load_task(module=r.get('task_module'),
                              task_name=r['task_family'],
                              params_str=r['task_params'])
            except TaskClassException as ex:
                self._handle_task_load_error(ex, [task_id])
                task_id = None
                self.run_succeeded = False

        if task_id is not None and 'batch_task_ids' in r:
            batch_tasks = [task for task in (
                self._scheduled_tasks.get(batch_id) for batch_id in r['batch_task_ids'])
                if task is not None]
            self._batch_running_tasks[task_id] = batch_tasks

        if task_id is not None:
            self._task_execution_tokens[task_id] = r.get('task_token')

        return GetWorkResponse(
            task_id=task_id,
            running_tasks=running_tasks,
            n_pending_tasks=r['n_pending_tasks'],
            n_unique_pending=r['n_unique_pending'],

            # TODO: For a tiny amount of time (a month?) we'll keep forwards compatibility
            #  That is you can user a newer client than server (Sep 2016)
            n_pending_last_scheduled=r.get('n_pending_last_scheduled', 0),
            worker_state=r.get('worker_state', WORKER_STATE_ACTIVE),
        )

    def _run_task(self, task_id):
        if task_id in self._running_tasks:
            logger.debug('Got already running task id {} from scheduler, taking a break'.format(task_id))
            next(self._sleeper())
            return

        cancel = self._cancel_requests.get(task_id)
        if cancel is not None and cancel['token'] == self._task_execution_tokens.get(task_id):
            # the scheduler cancelled this dispatch before the task was started
            logger.info('Not starting task %s: its execution was cancelled', task_id)
            self._task_execution_tokens.pop(task_id, None)
            self._report_task_cancelled(task_id, cancel['token'])
            return

        task = self._scheduled_tasks[task_id]

        task_process = self._create_task_process(task)

        self._running_tasks[task_id] = task_process

        if task_process.use_multiprocessing:
            with fork_lock:
                task_process.start()
        else:
            # Run in the same process
            task_process.run()

    def _create_task_process(self, task):
        message_queue = multiprocessing.Queue() if task.accepts_messages else None
        reporter = TaskStatusReporter(self._scheduler, task.task_id, self._id, message_queue)
        use_multiprocessing = self._config.force_multiprocessing or bool(self.worker_processes > 1)
        return ContextManagedTaskProcess(
            self._config.task_process_context,
            task, self._id, self._task_result_queue, reporter,
            use_multiprocessing=use_multiprocessing,
            worker_timeout=self._config.timeout,
            check_unfulfilled_deps=self._config.check_unfulfilled_deps,
            check_complete_on_run=self._config.check_complete_on_run,
            task_completion_cache=self._task_completion_cache,
            execution_token=self._task_execution_tokens.get(task.task_id),
        )

    def _report_task_cancelled(self, task_id, task_token):
        """
        Tell the scheduler that the execution it cancelled has stopped.

        The cancel request is only dropped once the report went through, so a
        failed report is retried on a later pass of _process_cancels().
        """
        self.run_succeeded = False
        self._add_task(worker=self._id,
                       task_id=task_id,
                       status=CANCELLED,
                       runnable=None,
                       task_token=task_token)
        self._cancel_requests.pop(task_id, None)

    def _reject_task_cancel(self, task_id, task_token):
        """
        Tell the scheduler that this execution cannot be cancelled after all
        (it is not running in its own process). The scheduler then voids the
        cancellation and leaves the execution running.
        """
        self._scheduler.reject_cancel_task(task_id=task_id, worker=self._id,
                                           task_token=task_token)
        self._cancel_requests.pop(task_id, None)

    def _process_cancels(self):
        """
        Advance all pending cancellation requests without blocking.

        Terminates (and, after ``cancel_timeout``, kills) the processes of
        cancelled tasks, reaps them once they have exited, and only then
        confirms the cancellation to the scheduler. Runs in the main worker
        loop so neither heartbeats nor sibling tasks are blocked by waiting
        for a process to exit.
        """
        if not self._cancel_requests:
            return

        now = time.time()
        for task_id, cancel in list(self._cancel_requests.items()):
            token = cancel['token']
            task_process = self._running_tasks.get(task_id)

            if task_process is None:
                # The execution is not running here (already finished or never
                # started): nothing left to stop, confirm right away.
                self._report_task_cancelled(task_id, token)
                continue

            if self._task_execution_tokens.get(task_id) != token:
                # Stale request aimed at a previous execution of this task id;
                # the current execution must not be touched.
                logger.debug('Discarding stale cancel request for %s', task_id)
                self._cancel_requests.pop(task_id, None)
                continue

            if not task_process.use_multiprocessing:
                # Safety net: the scheduler only sends cancel requests for
                # tasks running in their own process, but a per-task
                # worker_timeout=0 (or a worker_processes downgrade) can make
                # this false. We must not kill the whole worker, so the
                # cancellation is rejected and the execution keeps running.
                logger.error('Cannot cancel %s: task is not running in a separate process', task_id)
                self._reject_task_cancel(task_id, token)
                continue

            if not task_process.is_alive():
                task_process.join()  # reap the child process
                self._running_tasks.pop(task_id)
                self._task_execution_tokens.pop(task_id, None)
                self._report_task_cancelled(task_id, token)
            elif not cancel['term_sent']:
                logger.info('Terminating process of cancelled task %s (pid %s)',
                            task_id, task_process.pid)
                try:
                    # non-blocking SIGTERM to the task process only; note we
                    # deliberately bypass TaskProcess.terminate() which waits
                    multiprocessing.Process.terminate(task_process)
                except Exception:
                    logger.exception('Failed to terminate process of task %s, will retry', task_id)
                else:
                    cancel['term_sent'] = True
                    cancel['deadline'] = now + self._config.cancel_timeout
            elif not cancel['kill_sent'] and now >= cancel['deadline']:
                logger.warning('Cancelled task %s did not exit within cancel_timeout of %ss, '
                               'killing process %s', task_id, self._config.cancel_timeout,
                               task_process.pid)
                try:
                    task_process.kill()  # non-blocking SIGKILL
                except Exception:
                    logger.exception('Failed to kill process of task %s, will retry', task_id)
                else:
                    cancel['kill_sent'] = True
            # once killed, the is_alive() check above reaps and confirms

    def _purge_children(self):
        """
        Find dead children and put a response on the result queue.

        :return:
        """
        for task_id, p in self._running_tasks.items():
            if task_id in self._cancel_requests:
                # being cancelled: _process_cancels() owns this process and
                # no failure may be reported for it
                continue
            if not p.is_alive() and p.exitcode:
                error_msg = 'Task {} died unexpectedly with exit code {}'.format(task_id, p.exitcode)
                p.task.trigger_event(Event.PROCESS_FAILURE, p.task, error_msg)
            elif p.timeout_time is not None and time.time() > float(p.timeout_time) and p.is_alive():
                p.terminate()
                error_msg = 'Task {} timed out after {} seconds and was terminated.'.format(task_id, p.worker_timeout)
                p.task.trigger_event(Event.TIMEOUT, p.task, error_msg)
            else:
                continue

            logger.info(error_msg)
            self._task_result_queue.put((task_id, FAILED, error_msg, [], [],
                                         self._task_execution_tokens.get(task_id)))

    def _handle_next_task(self):
        """
        We have to catch three ways a task can be "done":

        1. normal execution: the task runs/fails and puts a result back on the queue,
        2. new dependencies: the task yielded new deps that were not complete and
           will be rescheduled and dependencies added,
        3. child process dies: we need to catch this separately.
        """
        self._idle_since = None
        while True:
            self._purge_children()  # Deal with subprocess failures
            self._process_cancels()  # Advance task cancellations

            try:
                task_id, status, expl, missing, new_requirements, token = (
                    self._task_result_queue.get(
                        timeout=self._config.wait_interval))
            except Queue.Empty:
                return

            if task_id in self._cancel_requests:
                # being cancelled: the cancellation logic owns this task and
                # the result of the cancelled execution is irrelevant
                logger.debug('Discarding result of cancelled task %s', task_id)
                continue

            if token != self._task_execution_tokens.get(task_id):
                # result of a previous execution of this task id (e.g. put on
                # the queue just before a cancelled execution was stopped)
                logger.info('Discarding stale result for a previous execution of %s', task_id)
                continue

            task = self._scheduled_tasks[task_id]
            if not task or task_id not in self._running_tasks:
                continue
                # Not a running task. Probably already removed.
                # Maybe it yielded something?

            # external task if run not implemented, retry-able if config option is enabled.
            external_task_retryable = _is_external(task) and self._config.retry_external_tasks
            if status == FAILED and not external_task_retryable:
                self._email_task_failure(task, expl)

            new_deps = []
            if new_requirements:
                new_req = [load_task(module, name, params)
                           for module, name, params in new_requirements]
                for t in new_req:
                    self.add(t)
                new_deps = [t.task_id for t in new_req]

            self._add_task(worker=self._id,
                           task_id=task_id,
                           status=status,
                           expl=json.dumps(expl),
                           resources=task.process_resources(),
                           runnable=None,
                           params=task.to_str_params(),
                           family=task.task_family,
                           module=task.task_module,
                           new_deps=new_deps,
                           assistant=self._assistant,
                           retry_policy_dict=_get_retry_policy_dict(task),
                           task_token=self._task_execution_tokens.get(task_id))

            self._running_tasks.pop(task_id)
            self._task_execution_tokens.pop(task_id, None)

            # re-add task to reschedule missing dependencies
            if missing:
                reschedule = True

                # keep out of infinite loops by not rescheduling too many times
                for task_id in missing:
                    self.unfulfilled_counts[task_id] += 1
                    if (self.unfulfilled_counts[task_id] >
                            self._config.max_reschedules):
                        reschedule = False
                if reschedule:
                    self.add(task)

            self.run_succeeded &= (status == DONE) or (len(new_deps) > 0)
            return

    def _sleeper(self):
        # TODO is exponential backoff necessary?
        while True:
            jitter = self._config.wait_jitter
            wait_interval = self._config.wait_interval + random.uniform(0, jitter)
            logger.debug('Sleeping for %f seconds', wait_interval)
            time.sleep(wait_interval)
            yield

    def _keep_alive(self, get_work_response):
        """
        Returns true if a worker should stay alive given.

        If worker-keep-alive is not set, this will always return false.
        For an assistant, it will always return the value of worker-keep-alive.
        Otherwise, it will return true for nonzero n_pending_tasks.

        If worker-count-uniques is true, it will also
        require that one of the tasks is unique to this worker.
        """
        if not self._config.keep_alive:
            return False
        elif self._assistant:
            return True
        elif self._config.count_last_scheduled:
            return get_work_response.n_pending_last_scheduled > 0
        elif self._config.count_uniques:
            return get_work_response.n_unique_pending > 0
        elif get_work_response.n_pending_tasks == 0:
            return False
        elif not self._config.max_keep_alive_idle_duration:
            return True
        elif not self._idle_since:
            return True
        else:
            time_to_shutdown = self._idle_since + self._config.max_keep_alive_idle_duration - datetime.datetime.now()
            logger.debug("[%s] %s until shutdown", self._id, time_to_shutdown)
            return time_to_shutdown > datetime.timedelta(0)

    def handle_interrupt(self, signum, _):
        """
        Stops the assistant from asking for more work on SIGUSR1
        """
        if signum == signal.SIGUSR1:
            self._start_phasing_out()

    def _start_phasing_out(self):
        """
        Go into a mode where we dont ask for more work and quit once existing
        tasks are done.
        """
        self._config.keep_alive = False
        self._stop_requesting_work = True

    def run(self):
        """
        Returns True if all scheduled tasks were executed successfully.
        """
        logger.info('Running Worker with %d processes', self.worker_processes)

        sleeper = self._sleeper()
        self.run_succeeded = True

        self._add_worker()

        while True:
            self._process_cancels()  # Advance task cancellations
            while len(self._running_tasks) >= self.worker_processes > 0:
                logger.debug('%d running tasks, waiting for next task to finish', len(self._running_tasks))
                self._handle_next_task()

            get_work_response = self._get_work()

            if get_work_response.worker_state == WORKER_STATE_DISABLED:
                self._start_phasing_out()

            if get_work_response.task_id is None:
                if not self._stop_requesting_work:
                    self._log_remote_tasks(get_work_response)
                if len(self._running_tasks) == 0 and not self._cancel_requests:
                    self._idle_since = self._idle_since or datetime.datetime.now()
                    if self._keep_alive(get_work_response):
                        next(sleeper)
                        continue
                    else:
                        break
                else:
                    self._handle_next_task()
                    continue

            # task_id is not None:
            logger.debug("Pending tasks: %s", get_work_response.n_pending_tasks)
            self._run_task(get_work_response.task_id)

        while len(self._running_tasks):
            logger.debug('Shut down Worker, %d more tasks to go', len(self._running_tasks))
            self._handle_next_task()

        return self.run_succeeded

    def _handle_rpc_message(self, message):
        logger.info("Worker %s got message %s" % (self._id, message))

        # the message is a dict {'name': <function_name>, 'kwargs': <function_kwargs>}
        name = message['name']
        kwargs = message['kwargs']

        # find the function and check if it's callable and configured to work
        # as a message callback
        func = getattr(self, name, None)
        tpl = (self._id, name)
        if not callable(func):
            logger.error("Worker %s has no function '%s'" % tpl)
        elif not getattr(func, "is_rpc_message_callback", False):
            logger.error("Worker %s function '%s' is not available as rpc message callback" % tpl)
        else:
            logger.info("Worker %s successfully dispatched rpc message to function '%s'" % tpl)
            func(**kwargs)

    @rpc_message_callback
    def set_worker_processes(self, n):
        # set the new value
        self.worker_processes = max(1, n)

        # tell the scheduler
        self._scheduler.add_worker(self._id, {'workers': self.worker_processes,
                                              'cancel_support': self._supports_cancellation()})

    @rpc_message_callback
    def cancel_task(self, task_id, task_token=None):
        """
        Handle a cancel notification from the scheduler.

        Only records the request; the actual stopping is performed by the
        main worker loop in _process_cancels() so that this heartbeat
        callback never blocks.
        """
        self._note_cancel_request(task_id, task_token)

    def _note_cancel_request(self, task_id, task_token=None):
        task_id = str(task_id)
        if task_id not in self._cancel_requests:
            logger.info('Worker %s got cancel request for task %s', self._id, task_id)
            self._cancel_requests[task_id] = {
                'token': task_token,
                'term_sent': False,
                'kill_sent': False,
                'deadline': None,
            }

    @rpc_message_callback
    def dispatch_scheduler_message(self, task_id, message_id, content, **kwargs):
        task_id = str(task_id)
        if task_id in self._running_tasks:
            task_process = self._running_tasks[task_id]
            if task_process.status_reporter.scheduler_messages:
                message = SchedulerMessage(self._scheduler, task_id, message_id, content, **kwargs)
                task_process.status_reporter.scheduler_messages.put(message)
