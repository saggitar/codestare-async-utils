from __future__ import annotations

import asyncio
import typing as t

from ._typing import _T, _SimpleCoroutine, _S


def make_async(func: t.Callable[[_T], _SimpleCoroutine[_S] | _S]) -> t.Callable[[_T], _SimpleCoroutine[_S]]:
    """
    Decorator to turn a non async function into a coroutine by running it in the default executor pool.
    """

    if asyncio.iscoroutinefunction(func):
        return t.cast(t.Callable[[_T], _SimpleCoroutine[_S]], func)

    @wraps(func)
    async def _callback(*args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)

    return _callbac
