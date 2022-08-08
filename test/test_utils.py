from __future__ import annotations
import typing as t
import asyncio
from contextlib import suppress, AsyncExitStack, asynccontextmanager

import pytest
from codestare.async_utils import TaskNursery, CoroutineWrapper


class MockWrapper(CoroutineWrapper):
    def __init__(self, range: int | None = 20, sleep_time=0.1):
        self.value = None
        self.range = range
        super().__init__(coroutine=self._run(sleep_time))

    async def _run(self, sleep_time):
        self.value = 0

        if self.range is not None:
            for i in range(self.range):
                self.value = i
                await asyncio.sleep(sleep_time)
        else:
            while True:
                self.value += 1
                await asyncio.sleep(sleep_time)


@pytest.fixture
async def nursery() -> TaskNursery:
    callback_ran = False

    async def callback():
        nonlocal callback_ran
        callback_ran = True

    nursery = TaskNursery()
    nursery.push_async_callback(callback)
    yield nursery

    if any(task.cancelled() for task in nursery.tasks):
        assert callback_ran


@pytest.mark.parametrize('future', [MockWrapper()])
async def test_nursery(future, nursery):
    callback_called = False

    def callback(task):
        nonlocal callback_called
        callback_called = True
        print(f"Callback called for {task}")

    task = nursery.create_task(future)
    task.add_done_callback(callback)
    task.add_done_callback(lambda _: nursery.sentinel_task.cancel())

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert callback_called


async def test_exit_stack(nursery):
    value = None

    @asynccontextmanager
    async def t_context_manager():
        nonlocal value
        value = True
        yield
        value = False

    async with nursery:
        await nursery.enter_async_context(t_context_manager())

    assert nursery.sentinel_task is None
    await asyncio.sleep(0)
    assert value is not None, "context was not entered"
    assert value is False, "context was not exited"


@pytest.mark.parametrize('future', [MockWrapper(range=None)])
async def test_swap_ctx_manager(nursery, future):
    async with nursery:
        task = nursery.create_task(future)
        new = nursery.pop_all()

    assert nursery.sentinel_task is not None
    assert not task.cancelled()

    await new.aclose()
    assert task.cancelled()
    assert nursery.sentinel_task is None



