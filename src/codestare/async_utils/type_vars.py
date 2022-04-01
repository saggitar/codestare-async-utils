from __future__ import annotations

from typing import TypeVar, Coroutine, Any

T = TypeVar('T')
"""
Simple TypeVar
"""
S = TypeVar('S')
"""
Simple TypeVar
"""
R = TypeVar('R')
"""
Simple TypeVar
"""
T_Exception = TypeVar('T_Exception', bound=Exception)
"""
bound to :class:`Exception`
"""
T_co = TypeVar('T_co', covariant=True)
"""
covariant values
"""
T_contra = TypeVar('T_contra', contravariant=True)
"""
contravariant values
"""
SimpleCoroutine = Coroutine[Any, Any, T]
"""
TypeAlias for a Coroutine that only cares about return Type
"""
