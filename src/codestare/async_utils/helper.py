from __future__ import annotations

import abc
import asyncio
import typing as t
from functools import wraps
from warnings import warn

from ._typing import _T, SimpleCoroutine, _S


def make_async(func: t.Callable[[_T], SimpleCoroutine[_S] | _S], await_all=True) -> t.Callable[[_T], SimpleCoroutine[_S]]:
    """
    Decorator to turn a non async function into a coroutine by running it in the default executor pool.
    """

    if asyncio.iscoroutinefunction(func):
        return t.cast(t.Callable[[_T], SimpleCoroutine[_S]], func)

    @wraps(func)
    async def _callback(*args):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, func, *args)

        if asyncio.iscoroutine(result):
            warn(f"Non async callback {func} returned a coroutine. Awaiting it directly since await_all={await_all}")
            while asyncio.iscoroutine(result):
                result = await result

        return result

    return _callback


class RegistryMeta(abc.ABCMeta):
    """
    Set __unique_key_attr__ in created classes to some attribute of the class instance that is
    unique to use this attribute as a key in the registry instead of the __default_key__.
    If the __default_key__ attribute is not present in the instance, it will be copied from the class
    on instance creation, and appended with the number of instances created before.
    """
    __default_key__: str = '__name__'
    __full_name__: str = None

    @staticmethod
    def _post_new(instance: t.Any):
        cls: RegistryMeta = type(instance)
        cls.__created__.append(instance)
        if not hasattr(instance, cls.__unique_key_attr__) and cls.__unique_key_attr__ == RegistryMeta.__default_key__:
            value = f"{cls.__default_value__}_{len(cls.registry.get(cls.__default_value__, ()))}"
            setattr(instance, cls.__unique_key_attr__, value)

    @property
    def registry(cls: t.Type[_T]) -> t.Dict[t.Any, _T]:
        return {
            getattr(instance, cls.__unique_key_attr__): instance
            for instance in cls.__created__
            if getattr(instance, cls.__unique_key_attr__, None) is not None
        }

    @staticmethod
    def _wrap_new(__new__):
        @wraps(__new__)
        def wrapped(*args, **kwargs):
            instance = __new__(*args, **kwargs)
            RegistryMeta._post_new(instance)
            return instance

        wrapped.__is_wrapped__ = True
        return wrapped

    def __new__(mcs, name, bases, attrs):
        kls: RegistryMeta = super().__new__(mcs, name, bases, attrs)
        if not hasattr(kls, '__created__'):
            kls.__created__ = []

        if not hasattr(kls, '__unique_key_attr__'):
            kls.__unique_key_attr__ = mcs.__default_key__

        if not hasattr(kls, '__new_is_wrapped__') and not hasattr(kls.__new__, '__is_wrapped__'):
            kls.__new__ = mcs._wrap_new(kls.__new__)

        kls.__default_value__ = f"{attrs['__module__']}.{attrs['__qualname__']}"

        return kls


class Registry(object, metaclass=RegistryMeta):
    __new_is_wrapped__ = True

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        assert isinstance(instance, Registry)
        cls._post_new(instance)
        return instance
