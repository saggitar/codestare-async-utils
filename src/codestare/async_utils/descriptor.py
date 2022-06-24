from __future__ import annotations

import asyncio
import collections
import typing
import warnings

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

import functools

try:
    from functools import cached_property
except ImportError:
    from backports.cached_property import cached_property

from . import helper
from .type_vars import T_co, T, T_contra


# pycharm is to stupid to understand this as of now
# https://youtrack.jetbrains.com/issue/PY-29257
class fget_type(Protocol[T_contra]):
    def __call__(self) -> T_contra:
        """
        :class:`fget_type` callables need to have this signature
        """


class fset_type(Protocol[T_co]):
    def __call__(self, value: T_co) -> None:
        """
        :class:`fset_type` callables need to have this signature
        """


class accessor(typing.Generic[T]):
    """
    An accessor provides easy shared access to a resource and

    Example:

        In the simplest case, an accessor synchronizes reads and writes out of the box ::

            >>> import asyncio
            >>> from codestare.async_utils import accessor
            >>> foo = accessor()
            >>> async def wait_for_write(accessor_):
            ...     print(await accessor_.get())
            ...
            >>> background = asyncio.create_task(wait_for_write(foo))
            >>> await foo.set("Bar")
            Bar

        It's possible to use custom getters / setter e.g. create an :class:`accessor` to the value managed
        by a normal property if one needs shared access as well ::

            >>> class Thing:
            ...     def __init__(self):
            ...         self._value = None
            ...     @property
            ...     def value(self):
            ...         return self._value
            ...     @value._setter
            ...     def value(self, val):
            ...         if not val:
            ...             raise ValueError(f"Illegal value {val}")
            ...         self._value = val
            ...
            >>> thing = Thing()
            >>> thing.value = 3
            >>> thing.value
            3
            >>> thing.value = 0
            ValueError: Illegal value 0
            >>> class BetterThing(Thing):
            ...     def __init__(self):
            ...         super().__init__()
            ...         self.value_accessor = accessor(funcs=(
            ...             type(self).value.fget.__get__(self),
            ...             type(self).value.fset.__get__(self)
            ...         ))
            ...
            >>> better_thing = BetterThing()
            >>> background = asyncio.create_task(wait_for_write(better_thing.value_accessor))
            >>> await better_thing.value_accessor.set(3)
            3
            >>> better_thing.value
            3
            >>> await better_thing.value_accessor.set(0)
            ValueError: Illegal value 0

    See Also:
        :class:`condition_property` -- decorator to create `accessor properties` more easily

    """
    fget: 'fget_type[T]'
    fset: 'fset_type[T]'

    @typing.overload
    def __init__(self: accessor[typing.Any],
                 *,
                 funcs: None = ...,
                 condition: asyncio.Condition | None = None,
                 name: str = None
                 ):
        ...

    @typing.overload
    def __init__(self: accessor[T],
                 *,
                 condition: asyncio.Condition | None = None,
                 name: str = None
                 ):
        ...

    @typing.overload
    def __init__(self: accessor[T],
                 *,
                 funcs: typing.Tuple[fget_type[T], fset_type[T]] = ...,
                 condition: asyncio.Condition | None = None,
                 name: str = None
                 ):
        ...

    def __init__(self,
                 *,
                 funcs: typing.Tuple | None = None,
                 condition: asyncio.Condition | None = None,
                 name: str = None
                 ):
        """
        Args:
            funcs: getter and setter for some value -- `optional`, if not passed get / set a private field of the
                object
            condition: condition to synchronize access to the value -- `optional`, if not passed a new condition
                is created
        """
        if funcs is None:
            def set(instance, value):
                instance._value = value

            def get(instance):
                return instance._value

            self._value = None
            fget = get.__get__(self, type(self))
            fset = set.__get__(self, type(self))
        else:
            non_callable = [f for f in funcs if not callable(f)]
            if non_callable:
                raise ValueError(f"parameters {non_callable} passed as ``funcs`` tuple are not callable")
            fget, fset = funcs

        self.fset = fset
        """
        Setter, either passed via ``funcs`` argument, or a setter of an internal value if no ``funcs`` where passed
        """
        self.fget = fget
        """
        Getter, either passed via ``funcs`` argument, or a getter of an internal value if no ``funcs`` where passed
        """
        self.condition: asyncio.Condition = condition or asyncio.Condition()
        """
        Used to synchronized access, either passed via ``condition`` argument, or a new condition created specifically
        for this accessor
        """
        self.name = name
        """
        For debug purposes
        """

    @property
    def value(self) -> T | None:
        """
        Simple access to the value produced by :attr:`.fget` without async locks i.e. not safe if you did not
        acquire the lock of :attr:`.condition`
        """
        return self.fget()

    async def set(self, value: T) -> None:
        """
        Sets the value (using :attr:`.fset`) and notifies every coroutine waiting on the :attr:`.condition` (e.g.
        :meth:`.get`

        Args:
            value: new value passed to :attr:`.fset`
        """
        async with self.condition:
            self.fset(value)
            self.condition.notify_all()

    @property
    def has_waiter(self):
        waiters: collections.deque = getattr(self.condition, '_waiters', None)
        if waiters is None:
            warnings.warn(f"has_waiter relies on a private attribute of asyncio.Condition which"
                          f" cannot be accessed. Please report this. Until this has been addressed"
                          f" has_waiters will always assume waiters, which might lead to unexpected behaviour.")

            predicate = lambda: True
        else:
            predicate = functools.partial(bool, waiters)

        return helper.awaitable_predicate(predicate=predicate, condition=self.condition)

    async def get(self,
                  *,
                  predicate: typing.Callable[[T], bool] | None = None,
                  await_next_write: bool = False) -> T:
        """
        Shared access to value produced by :attr:`.fget`

        Args:
            predicate: waits for the predicate result to be truthy, then returns the result of :attr:`.fget`.
                The default predicate (used when ``predicate=None``) returns ``[False, True, True, ...]``, so
                :meth:`.get` blocks once, until it is notified from a :meth:`.set` and then does not block again.
                Passing ``predicate=(lambda: True)`` will make :meth:`.get` not block at all.
            await_next_write: if set to ``True``, and a predicate is passed, the predicate will only be applied
                once the default predicate (see above) also returns ``True`` i.e. you get the next value that matches
                the predicate, even if the current value also matches -- **optional**

        Returns:
            value produced by :attr:`.fget`

        Raises:
            ValueError: if ``predicate`` is not a callable

        See Also:
            :meth:`asyncio.Condition.wait_for` -- used to wait for internal condition
        """
        if predicate is None and not await_next_write:
            await_next_write = True

        # this predicate returns False, True i.e. it will block once and always return after notify
        wait_predicate = iter([False, True]).__next__ if await_next_write else None

        if predicate is not None and not callable(predicate):
            raise ValueError(f"{predicate} is not callable")

        def notifying_predicate(acc: accessor):
            acc.has_waiter.condition.notify_all()

            use_value = True if not wait_predicate else wait_predicate()
            matching_value = True if not predicate else predicate(acc.fget())
            return use_value and matching_value

        async with self.condition:
            await self.condition.wait_for(notifying_predicate.__get__(self, None))
            return self.fget()

    def __repr__(self):
        params = {param: getattr(self, param, None) for param in ['name', 'fget', 'fset']}
        return (f"<{self.__class__.__name__} object "
                f"[{', '.join('{}={!r}'.format(name, value) for name, value in params.items())}]>")


class condition_property(cached_property, typing.Generic[T]):
    """
    This is a decorator to create a cached :class:`accessor` to handle access to some data via a
    :class:`asyncio.Condition`.

    You can use it like the normal `@property` decorator, but the result of the lookup (`__get__` of the descriptor)
    will be an :class:`accessor` with coroutine attributes to handle safely setting and getting the value
    (from the objects methods passed via ``setter`` and ``getter``, like in normal properties) by means of a condition.

    See Also:
        :class:`accessor` -- how to access the value
    """

    def __init__(self: condition_property[T],
                 fget: typing.Callable[[typing.Any], T] | None = None,
                 fset: typing.Callable[[typing.Any, T], None] | None = None,
                 fdel: typing.Callable[[typing.Any], None] | None = None,
                 doc: str | None = None) -> None:
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

        super().__init__(self._create_accessor)

    def _create_accessor(self: 'condition_property[T]', obj: object) -> accessor[T]:
        return accessor(
            funcs=(functools.partial(self._get, obj), functools.partial(self._set, obj),), name=self.attrname
        )

    def _set(self, obj: object, value: T):
        if self.fset is None:
            raise AttributeError(f"can't set attribute {self.attrname}")
        self.fset(obj, value)

    def _get(self, obj: object):
        if self.fget is None:
            raise AttributeError(f'unreadable attribute {self.attrname}')

        if self.fset is None:
            raise AttributeError(f"`get` will block until the next value is set, but no setter is defined.")

        return self.fget(obj)

    def __set__(self, obj, value: T):
        raise AttributeError(f"can't set {self.attrname} directly, use set()")

    @typing.overload
    def __get__(self, instance: None, owner: typing.Type[typing.Any] | None = None) -> condition_property[T]:
        ...

    @typing.overload
    def __get__(self, instance: object, owner: typing.Type[typing.Any] | None = None) -> accessor[T]:
        ...

    def __get__(self, instance: object | None,
                owner: typing.Type[typing.Any] | None = None) -> accessor[T] | condition_property[T]:
        if instance is None:
            return typing.cast(condition_property[T], super().__get__(instance, owner))
        else:
            return typing.cast(accessor[T], super().__get__(instance, owner))

    def getter(self: condition_property[T], fget: typing.Callable[[typing.Any], T]) -> condition_property[T]:
        """
        This is a cached property but uses the same interface as a normal :obj:`property`.
        See example of :obj:`property` documentation on how to use.
        """
        prop = type(self)(fget, self.fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def setter(self: condition_property[T], fset: typing.Callable[[typing.Any, T], None]) -> condition_property[T]:
        """
        This is a cached property but uses the same interface as a normal :obj:`property`.
        See example of :obj:`property` documentation on how to use.
        """
        prop = type(self)(self.fget, fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def deleter(self: condition_property[T], fdel) -> condition_property[T]:
        """
        This is a cached property but uses the same interface as a normal :obj:`property`.
        See example of :obj:`property` documentation on how to use.
        """
        prop = type(self)(self.fget, self.fset, fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop
