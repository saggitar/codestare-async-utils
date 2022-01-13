from __future__ import annotations

import asyncio
import typing as t
from functools import cached_property, partial

from ._typing import _T

# pycharm is to stupid to understand this as of now
# https://youtrack.jetbrains.com/issue/PY-29257
_fget_type = t.Callable[[], _T]
_fset_type = t.Callable[[_T], None]


class accessor(t.Generic[_T]):
    """
    The ``get`` coroutine of the accessor takes an optional predicate callable (or None, see below),
    and waits for the predicate result to be truthy, then returns the result of the getter. (see the ``wait_for``
    documentation of asyncio.Condition for more info on the predicate callable).
    The default predicate (used when ``predicate=None``) returns [False, True, True, ...], so ``get`` blocks once,
    until it is notified from a ``set`` and then does not block again.
    Passing ``predicate=(lambda: True)`` will make ``get`` not block at all.

    The ``set`` coroutine of the accessor sets the value and notifies every ``get``.
    """
    fget: _fget_type[_T]
    fset: _fset_type[_T]

    @t.overload
    def __init__(self: accessor[t.Any],
                 *,
                 funcs: None = ...,
                 condition: asyncio.Condition | None = None,
                 ):
        ...

    @t.overload
    def __init__(self: accessor[_T],
                 *,
                 condition: asyncio.Condition | None = None,
                 ):
        ...

    @t.overload
    def __init__(self: accessor[_T],
                 *,
                 funcs: t.Tuple[_fget_type[_T], _fset_type[_T]] = ...,
                 condition: asyncio.Condition | None = None,
                 ):
        ...

    def __init__(self,
                 *,
                 funcs: t.Tuple | None = None,
                 condition: asyncio.Condition | None = None,
                 ):
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
        self.fget = fget
        self.condition = condition or asyncio.Condition()

    @property
    def value(self) -> _T | None:
        return self.fget()

    async def set(self, value: _T):
        async with self.condition:
            self.fset(value)
            self.condition.notify_all()

    async def get(self, *, predicate: t.Callable[[_T], bool] | None = None) -> _T:
        if predicate is None:
            # this predicate returns False, True i.e. it will block once and always return after notify
            _get_value = iter([False, True]).__next__
            predicate = (
                lambda _: _get_value()
            )

        if not callable(predicate):
            raise ValueError(f"{predicate} is not callable")

        async with self.condition:
            await self.condition.wait_for(lambda: predicate(self.fget()))
            return self.fget()


class condition_property(cached_property, t.Generic[_T]):
    """
    This is a decorator to create a cached ``accessor`` to handle access to some data via a asyncio.Condition
    You can use it like the normal @property decorator, but the result of the lookup (__get__ of the descriptor) will
    be an ``accessor`` with coroutine attributes to handle safely setting and getting the value (from the objects
    methods passed via ``setter`` and ``getter`` , like in normal properties) by means of a condition.

    """

    def __init__(self: condition_property[_T],
                 fget: t.Callable[[t.Any], _T] | None = None,
                 fset: t.Callable[[t.Any, _T], None] | None = None,
                 fdel: t.Callable[[t.Any], None] | None = None,
                 doc: str | None = None) -> None:
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

        super().__init__(self._create_accessor)

    def _create_accessor(self: 'condition_property[_T]', obj: object) -> accessor[_T]:
        return accessor(
            funcs=(partial(self._get, obj), partial(self._set, obj),)
        )

    def _set(self, obj: object, value: _T):
        if self.fset is None:
            raise AttributeError(f"can't set attribute {self.attrname}")
        self.fset(obj, value)

    def _get(self, obj: object):
        if self.fget is None:
            raise AttributeError(f'unreadable attribute {self.attrname}')

        if self.fset is None:
            raise AttributeError(f"`get` will block until the next value is set, but no setter is defined.")

        return self.fget(obj)

    def __set__(self, obj, value: _T):
        raise AttributeError(f"can't set {self.attrname} directly, use set()")

    @t.overload
    def __get__(self, instance: None, owner: t.Type[t.Any] | None = None) -> condition_property[_T]:
        ...

    @t.overload
    def __get__(self, instance: object, owner: t.Type[t.Any] | None = None) -> accessor[_T]:
        ...

    def __get__(self, instance: object | None,
                owner: t.Type[t.Any] | None = None) -> accessor[_T] | condition_property[_T]:
        if instance is None:
            return t.cast(condition_property[_T], super().__get__(instance, owner))
        else:
            return t.cast(accessor[_T], super().__get__(instance, owner))

    def getter(self: condition_property[_T], fget: t.Callable[[t.Any], _T]) -> condition_property[_T]:
        prop = type(self)(fget, self.fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def setter(self: condition_property[_T], fset: t.Callable[[t.Any, _T], None]) -> condition_property[_T]:
        prop = type(self)(self.fget, fset, self.fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop

    def deleter(self: condition_property[_T], fdel) -> condition_property[_T]:
        prop = type(self)(self.fget, self.fset, fdel, self.__doc__)
        prop.attrname = self.attrname
        return prop
