from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
import signal
import traceback
import typing
import warnings

import sys

from . import helper
from .type_vars import T

log = logging.getLogger(__name__)

_TaskYieldType = typing.Optional[asyncio.Future]

__sentinel_tasks__ = set()
WINDOWS = sys.platform == 'win32'


class ExcInfo(typing.NamedTuple):
    """
    As returned by :func:`sys.exc_info`
    """
    exc_type: typing.Optional[typing.Type[Exception]] = None
    exc: typing.Optional[Exception] = None
    traceback: typing.Optional[typing.Any] = None


def mark_sentinel_task(task: asyncio.Task) -> asyncio.Task:
    """
    The package keeps a global reference to all tasks marked as a `sentinel task` i.e. they
    will not be garbage collected even if they are finished. Those tasks will be stopped before
    other tasks if you set up the shutdown handling with this module. These tasks are
    used by the :class:`TaskNursery` to trigger the nursery's shutdown by simply
    canceling / stopping the :attr:`~TaskNursery.sentinel_task`.

    When a :func:`shutdown` task is created (e.g. when a specific signal is received), all sentinel tasks
    will be stopped first -- this will trigger the shutdown of all :class:`TaskNurserys <TaskNursery>` that were
    created in the running interpreter.

    Args:
        task: will be globally referenced to be shut down eventually

    Returns:
        the task
    """
    global __sentinel_tasks__
    __sentinel_tasks__.add(task)
    return task


def handle_exception(loop: asyncio.AbstractEventLoop, context) -> None:
    """
    Calls the loops default exception handler, then creates a :func:`shutdown` task for the loop.
    This is the default exception handler that is set up for a loop with :func:`setup_shutdown_handling` if
    no other specific exception handler is set.

    Args:
        loop: event loop to shut down
        context: meta information for :func:`shutdown` task

    """
    # context["message"] will always be there; but context["exception"] may not
    loop.default_exception_handler(context)

    msg = context.get("tb", context["message"])
    log.error(msg)
    log.info("Shutting down...")

    additional_args = {'name': f"shutdown({context['message']})"} if sys.version_info >= (3, 8) else {}
    asyncio.create_task(shutdown(loop, context=context), **additional_args)


async def stop_task(task: asyncio.Task) -> typing.Any:
    """
    Try to cancel the task, and await it -- returns possible exceptions raised inside the task, e.g. the
    :class:`asyncio.CancelledError` raised by canceling the task prematurely.

    Args:
        task: task to stop, can't be the current running task

    Returns:
        result of task

    Raises:
        ValueError: if the currently running task is passed
    """
    if task is asyncio.current_task():
        raise ValueError(f"Not allowed to stop task running `stop_task`!")

    task.cancel('Stopped during shutdown')
    result = await asyncio.gather(task, return_exceptions=True)
    return result[0]


def setup_shutdown_handling(loop: asyncio.AbstractEventLoop) -> None:
    """
    Add signal handling and exception handling for the loop.
    On Windows only :obj:`signal.SIGBREAK` and :obj:`signal.SIGINT` are usable, on Linux add handling for
    :obj:`signal.SIGHUP`, :obj:`signal.SIGTERM` and :obj:`signal.SIGINT`.

    Uses normal signal handlers using :mod:`signal` except of :func:`asyncio.loop.add_signal_handler` since the
    latter is not available on Windows systems.

    The signal handlers registered start a :func:`shutdown` task for the passed ``loop``.
    Also sets :func:`handle_exception` as the loop's exception handler if no other handler is already set.

    Args:
        loop: event loop

    """
    _signals = (signal.SIGBREAK, signal.SIGINT) if WINDOWS else (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)

    def _clear_signal_handlers():
        remove_handler = (
            lambda sig: signal.signal(sig, signal.SIG_IGN)
            if WINDOWS else
            loop.remove_signal_handler
        )
        [remove_handler(s) for s in _signals]

    def signal_handler(sig, frame=None):
        kwargs = {} if sys.version_info < (3, 8) else {'name': f'shutdown({sig!r})'}
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as e:
            warnings.warn(f"Can't create shutdown task - {e}")
        else:
            loop.create_task(shutdown(loop, signal=sig, frame=frame), **kwargs)

    _clear_signal_handlers()

    [signal.signal(sig, signal_handler) for sig in _signals]

    if not loop.get_exception_handler():
        loop.set_exception_handler(handle_exception)


async def shutdown(loop: asyncio.AbstractEventLoop, **kwargs) -> None:
    """
    Await shutdown of the loop.

    If the :func:`shutdown` task is triggered by an exception from a task managed by a :class:`TaskNursery`, the
    ``**kwargs`` will contain a reference to the nursery's :attr:`~TaskNursery.sentinel_task` in the ``context``
    argument. If this is the case, this sentinel task will be stopped (using :func:`stop_task`) which triggers
    trigger the shutdown handling of the :class:`TaskNursery`.

    If the :func:`shutdown` task is triggered by a signal handler, all tasks
    :func:`marked as sentinel tasks <mark_sentinel_task>` will be stopped first, instead.

    Finally all tasks still running in the ``loop`` will be stopped.
    Then the loop will be :meth:`stopped <asyncio.loop.stop>`.

    Args:
        loop: event loop that needs to be stopped
        **kwargs: meta information like received signal or exception context

    """
    sig = kwargs.pop('signal', None)

    if sig:
        log.info(f"Received exit signal {sig!r}...")

    def get_tasks():
        non_current = [
            task_ for task_ in asyncio.all_tasks()
            if task_ is not asyncio.current_task()
        ]
        return non_current

    context = kwargs.pop('context', {})
    sentinel = context.get('sentinel')
    if sentinel:
        result = await stop_task(sentinel)
    else:
        is_sentinel = (lambda task_: task_ in __sentinel_tasks__)

        result = await asyncio.gather(
            *map(stop_task, filter(is_sentinel, get_tasks()))
        )
    if result:
        log.debug(f"Sentinel task[s] stopped with result {result!r}")

    tasks = get_tasks()
    log.info(f"Cancelling {len(tasks)} outstanding tasks")
    for task in tasks:
        await stop_task(task)

    loop.stop()


class TaskNursery(contextlib.AsyncExitStack, helper.Registry):
    """
    Use a :class:`TaskNursery` to create and manage tasks, instead of using the :meth:`asyncio.loop.create_task`
    method directly.

    All tasks created by a nursery receive a callback to execute when they finish, which will trigger
    a graceful shutdown of the event loop if necessary -- this saves a lot of boilerplate code

    Basically, a :class:`TaskNursery` is a :class:`contextlib.AsyncExitStack` that gets closed when it's
    :attr:`~.sentinel_task` is cancelled. This means you can enter any number of async context managers
    with a nursery, and every one of them will be closed when the nursery shuts down.

    Creating a :class:`TaskNursery` for an event loop also sets up signal handling and exception handling
    for that loop, i.e. if a loop has an active :class:`TaskNursery` exiting the interpreter forcefully (e.g.
    ``Ctrl+C`` in the shell) will gracefully shut down all nurseries first.

    :class:`TaskNursery` is a :class:`~codestare.async_utils.helper.Registry`
    with unique attribute :attr:`~TaskNursery.name` by default, i.e. all instances need to have a unique name
    and can be referenced by name using the :attr:`TaskNursery.registry` ::

        >>> from codestare.async_utils import TaskNursery
        >>> import asyncio
        >>> loop = asyncio.get_event_loop_policy().get_event_loop()
        >>> nursery = TaskNursery(name="Foo", loop=loop)
        >>> TaskNursery.registry
        {'Foo': codestare.async_utils.nursery.TaskNursery(name='Foo', loop=<...>)}


    Attributes:
        registry: Inherited from :class:`codestare.async_utils.helper.Registry`

    """
    __has_handling__: typing.Set[asyncio.BaseEventLoop] = set()
    __unique_key_attr__ = 'name'
    """
    Defines which attribute of the nursery instances is used as unique key for registry mapping
    """

    @staticmethod
    def add_shutdown_handling(loop: asyncio.AbstractEventLoop) -> None:
        """
        Add shutdown handling to the loop, once.
        Caches loops that were passed as argument to this method, additional calls with the
        same loop will be ignored.

        Args:
            loop: gets shutdown handling applied with :func:`setup_shutdown_handling`

        """
        if loop not in TaskNursery.__has_handling__:
            setup_shutdown_handling(loop)

    def stop_task(self, task: asyncio.Task):
        """
        Try to stop a task managed by this nursery using :func:`stop_task`

        Args:
            task: should be stopped

        Returns:
            result of :func:`stop_task`
        Raises:
            ValueError: if the task is not managed by this nursery i.e. it was not created by :meth:`.create_task`
        """
        if task not in self._tasks:
            raise ValueError(f"{task} not contained in pending tasks of {self}")

        return stop_task(task)

    def __init__(self, name: str | typing.Callable[[], str] | None = None, loop: asyncio.BaseEventLoop | None = None):
        """
        Creates a nursery on the loop -- or the current running loop.

        Args:
            name: if a callable is used, the name can be dynamic
            loop: if no loop is passed the running loop is used
        """
        super().__init__()
        self._tasks: typing.List[asyncio.Task] = []

        self.loop: asyncio.BaseEventLoop = loop or asyncio.get_running_loop()
        """
        Event loop instance to start tasks in, either passed as ``loop`` or current running loop
        """
        # teardown behaviour
        self.push_async_callback(self._stop_all)
        self.add_shutdown_handling(self.loop)
        self.sentinel_task: asyncio.Task = mark_sentinel_task(
            self.create_task(
                self._sentinel_task(event=asyncio.Event()),  # dummy event which is never set
            )
        )
        """
        Dummy task that does nothing but cancelling it triggers the closing of this :class:`TaskNursery` i.e.
        all entered async contexts will be closed and all created tasks will be stopped.
        """
        self.sentinel_task.remove_done_callback(self._task_cb)
        self._name = name or f'TaskNursery-{len(type(self).registry)}'

    @property
    def name(self):
        """
        Unique name of nursery, used as :class:`~codestare.async_utils.Registry` key by default, see
        :attr:`.__unique_key_attr__`. If a callable is passed as `name` during initialization, this callable is
        used to compute the name dynamically.
        """
        value = self._name() if callable(self._name) else self._name
        return str(value)

    @property
    def tasks(self):
        """
        Reference to managed tasks
        """
        return self._tasks

    def pop_all(self: TaskNursery) -> TaskNursery:
        """
        Preserve the context stack by transferring it to a new instance.
        """
        try:
            new_stack: TaskNursery = super().pop_all()  # noqa
        except RuntimeError as e:
            if not str(e) == 'no running event loop':
                raise RuntimeError(f"Can't handle {e.__class__.__name__}") from e

            warnings.warn(f"Got exception {e} when creating new {self.__class__.__name__} instance."
                          f" Using the current loop {self.loop} instead.")

            new_stack = type(self)(
                name=self.name,
                loop=self.loop
            )
            new_stack._exit_callbacks = self._exit_callbacks  # noqa
            self._exit_callbacks = collections.deque()  # noqa

        new_stack._tasks = self._tasks
        return new_stack

    async def _sentinel_task(self, event):
        try:
            await event.wait()
        except asyncio.CancelledError:
            pass

        await self.aclose()

        if self.sentinel_task in self._tasks:
            self._tasks.remove(self.sentinel_task)

        self.sentinel_task = None
        return "Success"

    async def _stop_all(self):
        stoppable = [task for task in self._tasks if task is not asyncio.current_task()]
        results = await asyncio.gather(*map(self.stop_task, stoppable), return_exceptions=True)
        return results

    def _task_cb(self, task: asyncio.Task):
        try:
            result = task.result()
            return result
        except asyncio.CancelledError:
            pass
        except:  # noqa
            exc_info = sys.exc_info()
            self.loop.call_exception_handler(
                {
                    'message': f'{exc_info[1]!r} from {task!r}',
                    'task': task,
                    'tb': traceback.format_exc(),
                    'sentinel': self.sentinel_task
                }
            )
        finally:
            if task in self._tasks:
                self._tasks.remove(task)

    def create_task(self,
                    coro: typing.Generator[_TaskYieldType, None, T] | typing.Awaitable[T],
                    **kwargs) -> asyncio.Task[T]:
        """
        Tasks created with this method have a callback which runs when the task finishes and tries to retrieve
        the task result. If the task raised an error the :attr:`loops <.loop>` exception handler is called with
        the exception info and a reference to this nursery's :attr:`.sentinel_task`. Typically, the exception
        handler is :func:`handle_exception` (it is set as exception handler when the :class:`TaskNursery` is
        created for a loop without specific exception handler). It will cancel the `sentinel task` and
        trigger the nursery's shutdown.

        Args:
            coro: Coroutine which should run inside the task
            **kwargs: passed to :meth:`asyncio.loop.create_task`

        Returns:
            reference to created task
        """
        kls = self.__class__

        if hasattr(self, 'sentinel_task') and not self.sentinel_task:
            raise RuntimeError("Starting tasks with a task nursery that was already shut down. "
                               f"Create a new {kls.__module__}.{kls.__qualname__} instead")

        if hasattr(self, '_exit_callbacks') and not self._exit_callbacks:
            warnings.warn(f"Reusing a {kls.__module__}.{kls.__qualname__} that was already closed or 'popped' once, "
                          f"which should generally be avoided.")
            self.push_async_callback(self._stop_all)

        if sys.version_info >= (3, 8):
            if 'name' not in kwargs:
                kwargs['name'] = getattr(coro, '__name__', repr(coro))
            kwargs['name'] += f":{self.__registry_key__}"
        else:
            if kwargs.pop('name', None):
                warnings.warn(f"No `name` argument in {sys.version}")

        task = self.loop.create_task(coro, **kwargs)
        task.add_done_callback(self._task_cb)
        self._tasks.append(task)
        return task

    def __repr__(self):
        kls = self.__class__
        attrs = ['name', 'loop']

        return (f"{kls.__module__}.{kls.__qualname__}("
                f"{', '.join('{}={!r}'.format(name, getattr(self, name, None)) for name in attrs)}"
                f")")
