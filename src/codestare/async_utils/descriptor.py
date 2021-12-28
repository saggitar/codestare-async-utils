from __future__ import annotations

from itertools import chain, repeat
import asyncio
import typing as t
from dataclasses import dataclass
from functools import cached_property, partial

from ._typing import _T, _S, _T_cov, _T_con, _SimpleCoroutine

_NO_CALLABLE = object()


@dataclass(init=True)
class accessor(t.Generic[_S]):
    class getter_t(t.Protocol[_T_cov]):
        def __call__(self, *, predicate: t.Callable[[], bool] | None) -> _SimpleCoroutine[_T_cov]: ...

    class setter_t(t.Protocol[_T_con]):
        def __call__(self, value: _T_con) -> _SimpleCoroutine[None]: ...

    set: setter_t[_S]
    get: getter_t[_S]


class condition_property(cached_property, t.Generic[_T]):
    """
    This is a decorator to create a cached ``accessor`` to handle access to some data via a asyncio.Condition
    You can use it like the normal @property decorator, but the result of the lookup (__get__ of the descriptor) will
    be an ``accessor`` with coroutine attributes to handle safely setting and getting the value (from the objects
    methods passed via ``setter`` and ``getter`` , like in normal properties) by means of a condition.

    The ``get`` coroutine of the accessor takes an optional predicate callable (or None, see below),
    and waits for the predicate result to be truthy, then returns the result of the getter. (see the ``wait_for``
    documentation of asyncio.Condition for more info on the predicate callable).
    The default predicate returns [False, True, True, ...], so ``get`` blocks once, until it is notified
    from a ``set`` and then does not block again.
    Passing ``predicate=None`` will make ``get`` not block at all.

    The ``set`` coroutine of the accessor sets the value and notifies every ``get``.
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
        condition = asyncio.Condition()

        return accessor(
            get=lambda predicate=_NO_CALLABLE: self._get(condition=condition, obj=obj, predicate=predicate),
            set=partial(self._set, condition=condition, obj=obj),
        )

    @staticmethod
    def _make_default_predicate():
        return chain(repeat(False, 1), repeat(True)).__next__

    async def _set(self,
                   value: _T,
                   *,
                   condition: asyncio.Condition,
                   obj: object):
        if self.fset is None:
            raise AttributeError(f"can't set attribute {self.attrname}")
        async with condition:
            self.fset(obj, value)
            condition.notify_all()

    async def _get(self,
                   *,
                   predicate: t.Callable[[], bool] | None | object = _NO_CALLABLE,
                   condition: asyncio.Condition,
                   obj: object):

        if predicate == _NO_CALLABLE:
            predicate = self._make_default_predicate()

        if predicate is None:
            predicate = (lambda: True)

        if not callable(predicate):
            raise ValueError(f"{predicate} is not callable")

        if self.fget is None:
            raise AttributeError(f'unreadable attribute {self.attrname}')

        if self.fset is None:
            raise AttributeError(f"`get` will block until the next value is set, but no setter is defined.")

        async with condition:
            await condition.wait_for(predicate)
            return self.fget(obj)

    def __set__(self, obj, value: _T):
        raise AttributeError(f"can't set {self.attrname} directly, use set()")

    @t.overload
    def __get__(self, instance: None, owner: t.Type[t.Any] | None = None) -> condition_property[_T]:
        ...

    @t.overload
    def __get__(self, instance: object, owner: t.Type[t.Any] | None = None) -> accessor[_T]:
        ...

    def __get__(self, instance: object | None, owner: t.Type[t.Any] | None = None) -> accessor[_T] | condition_property[
        _T]:
        return super().__get__(instance, owner)

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

