from __future__ import annotations

import asyncio
import logging
import sys
import typing as t
from contextlib import suppress
from functools import partial
from warnings import warn

from ._typing import _T
from .helper import Registry
from .wrapper import CoroutineWrapper

log = logging.getLogger(__name__)


def sentinel_finished(sentinel_task: asyncio.Future):
    try:
        result = sentinel_task.result()
        return result
    except Exception:  # noqa
        msg = f"Exception from sentinel task {sentinel_task}"
        log.exception(msg)


class Sentinel(CoroutineWrapper):
    def __init__(self, event: asyncio.Event, killall_on_finish: t.Callable[[], bool] | None = None):
        super().__init__(coroutine=self._run())
        self.event = event
        self.callbacks: t.List[t.Awaitable[None]] = []
        self.killall = killall_on_finish or (lambda: False)

    async def _run(self):
        try:
            await self.event.wait()
        except asyncio.CancelledError:
            warn(f"Cancelling sentinel task!")
        finally:
            for callback in self.callbacks:
                await callback

            # kill all tasks
            if self.killall():
                for task_ in filter(lambda task: not task.done(), asyncio.all_tasks()):
                    task_.cancel()
                    with suppress(asyncio.CancelledError):
                        await task_


_T_ExceptionHandlers = t.MutableMapping[t.Tuple[t.Type[Exception], ...], t.Callable[[Exception], t.Any]]
_TaskYieldType = t.Optional[asyncio.Future]


class ExcInfo(t.NamedTuple):
    exc_type: t.Type[Exception] | None = None
    exc: Exception | None = None
    traceback: t.Any | None = None


class NurseryChainException(Exception):

    @staticmethod
    def chain_exception(nursery: TaskNursery, sentinel_task: asyncio.Task):
        try:
            try:
                result = sentinel_task.result()
                return result
            except Exception as e:
                raise NurseryChainException from e
        except NurseryChainException:
            nursery.exception_info = sys.exc_info()
            nursery.trigger_sentinel.set()


class TaskNursery(Registry):

    def __init__(self):
        self._tasks: t.List[asyncio.Task] = []
        self.trigger_sentinel = asyncio.Event()
        self._exc_info: ExcInfo = ExcInfo()

        self.exception_handlers: _T_ExceptionHandlers = {}
        self.fallback_handler = log.exception
        self._sentinel = Sentinel(event=self.trigger_sentinel, killall_on_finish=lambda: any(self.exception_info))
        self.add_sentinel_callback(
            self._stop_tasks(),
        )
        self.sentinel = asyncio.shield(self._sentinel)
        self.sentinel.add_done_callback(sentinel_finished)
        self._attached = set()

    def add_exception_handler(self,
                              handler: t.Callable[[ExcInfo], ...],
                              *exception_types: t.Type[Exception]):
        self.exception_handlers.update(**{exception_types: handler})

    def add_sentinel_callback(self, *callbacks: t.Awaitable[None]):
        self._sentinel.callbacks += list(callbacks)

    def _run_exception_handlers(self):
        matching_handlers = [handler for types, handler in self.exception_handlers.items()
                             if any(isinstance(self.exception_info.exc, t) for t in types)]

        for handler in matching_handlers:
            handler(*self.exception_info)

        if not matching_handlers and self.exception_info:
            self.fallback_handler("Fallback Exception Handler")

    async def _stop_tasks(self):
        for task in self._tasks:
            task.cancel()

        with suppress(asyncio.CancelledError):
            for task in self._tasks:
                await task

    @property
    def exception_info(self) -> ExcInfo:
        return self._exc_info

    @exception_info.setter
    def exception_info(self, exc_info):
        self._exc_info = ExcInfo(*exc_info)
        self.trigger_sentinel.set()

    def create_task(self, coro: t.Generator[_TaskYieldType, None, _T] | t.Awaitable[_T]) -> asyncio.Task[_T]:
        future = asyncio.ensure_future(coro)
        future.add_done_callback(self._task_cb)
        self._tasks += [future]
        return future

    def attach(self, parent: TaskNursery):
        """
        If the parent nursery sentinel is done, propagate the exception into this task nursery

        :param parent: another Task Nursery
        """
        if self in parent._attached:
            return

        parent.sentinel.add_done_callback(partial(NurseryChainException.chain_exception, self))
        parent._attached.add(self)

    def _task_cb(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa
            self.exception_info = sys.exc_info()
            self._run_exception_handlers()

        self._tasks.remove(task)
