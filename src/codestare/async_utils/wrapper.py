from __future__ import annotations

import typing

from .type_vars import T, S, R


class CoroutineWrapper(typing.Coroutine[T, S, R]):
    """
    Complex Coroutines are easy to implement with native ``def async`` coroutine syntax, but often require
    some smaller coroutines to compose. Inheriting from :class:`CoroutineWrapper`, a complex coroutine can encapsulate
    all it's dependencies and auxiliary methods.

    Note:
        The example uses an asyncio REPL (``python -m asyncio``)

    Example:

        Let's consider an async generator that generates iterables ::

            >>> async def generate_ranges(up_to: int):
            ...     for n in range(up_to):
            ...             yield range(n)
            ...
            >>> [list(r) async for r in generate_ranges(5)]
            [[], [0], [0, 1], [0, 1, 2], [0, 1, 2, 3]]

        Our coroutine should apply some processing to the values, and to make it interesting it should only
        do so to values matching an additional filter function, i.e. values for which the specified filter function
        returns ``False`` should be ignored ::

            >>> async def coro(process, filter_, ranges):
            ...     async for r in ranges:
            ...         for n in r:
            ...             if not filter_(n): continue
            ...             process(n)
            ...
            >>> await coro(print, lambda n: n % 2 == 0, generate_ranges(5))
            0
            0
            0
            2
            0
            2

        This is pretty concise and one would probably understand what's happening, but for the sake of the
        example we want to break this processing behaviour down into pieces, so that we know
        what's happening. One could e.g. define a custom awaitable ::

            >>> class coro:
            ...     def __init__(self, process, filter_, ranges):
            ...         self.filter = filter_
            ...         self.process = process
            ...         self.ranges = ranges
            ...         self._work = self.work()
            ...     async def make_flat_values(self):
            ...         async for r in self.ranges:
            ...             for n in r:
            ...                 yield n
            ...     async def filter_values(self):
            ...         async for value in self.make_flat_values():
            ...             if self.filter(value):
            ...                 yield value
            ...     async def work(self):
            ...         async for value in self.filter_values():
            ...             self.process(value)
            ...     def __await__(self):
            ...         return self._work.__await__()
            ...
            >>> await coro(print, lambda n: n % 2 == 0, generate_ranges(5))
            0
            0
            0
            2
            0
            2

        The problem is, that ``coro`` now isn't a coroutine, i.e. one can't create a task with it for example ::

            >>> import asyncio
            >>> await asyncio.create_task(coro(print, lambda n: n % 2 == 0, generate_ranges(5)))
            TypeError: a coroutine was expected, got <coro object>

        This is where the :class:`CoroutineWrapper` comes in:

            >>> from codestare.async_utils import CoroutineWrapper
            >>> class coro(CoroutineWrapper):
            ...     def __init__(self, process, filter_, ranges):
            ...         self.filter = filter_
            ...         self.process = process
            ...         self.ranges = ranges
            ...         super().__init__(coroutine=self.work())
            ...     async def make_flat_values(self):
            ...         async for r in self.ranges:
            ...             for n in r:
            ...                 yield n
            ...     async def filter_values(self):
            ...         async for value in self.make_flat_values():
            ...             if self.filter(value):
            ...                 yield value
            ...     async def work(self):
            ...         async for value in self.filter_values():
            ...             self.process(value)
            >>> await asyncio.create_task(coro(print, lambda n: n % 2 == 0, generate_ranges(5)))
            0
            0
            0
            2
            0
            2

        The ``coro`` objects now actually implement the `asyncio` coroutine interface

    """

    def __init__(self: CoroutineWrapper[T, S, R], *, coroutine: typing.Coroutine[T, S, R], **kwargs):
        self._coroutine = coroutine
        super().__init__(**kwargs)

    def __await__(self):
        """
        Proxy to internal coroutine object
        """
        return self._coroutine.__await__()

    def send(self, value):
        """
        Proxy to internal coroutine object
        """
        return self._coroutine.send(value)

    def throw(self, typ, val=None, tb=None):
        """
        Proxy to internal coroutine object
        """
        if val is None:
            return self._coroutine.throw(typ)
        elif tb is None:
            return self._coroutine.throw(typ, val)
        else:
            return self._coroutine.throw(typ, val, tb)

    def close(self):
        """
        Proxy to internal coroutine object
        """
        return self._coroutine.close()
