# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import datetime
from functools import lru_cache, wraps

from frozendict import frozendict

from . import utils
from .persistent_lru_cache import persistent_lru_cache
from .utils import calc


def interval(seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0, maxsize=128, persistent="", custom=None, **kwargs):
    """Expires all entries in the cache every interval"""
    if not any((seconds, minutes, hours, days, weeks, months, years)):
        seconds = 1

    def _wrapper(foo):
        last = datetime.datetime.now()

        if custom:
            foo = custom(**kwargs)(foo)
        elif persistent:
            foo = persistent_lru_cache(persistent, maxsize=maxsize)(foo)
        else:
            foo = lru_cache(maxsize)(foo)

        @wraps(foo)
        def _wrapped_foo(*args, **kwargs):
            nonlocal last

            now = datetime.datetime.now()
            if (now - last).total_seconds() > calc(seconds, minutes, hours, days, weeks, months, years) or utils.TEMPORAL_CACHE_GLOBAL_DISABLE:
                foo.cache_clear()
            last = now

            args = tuple([frozendict(arg) if isinstance(arg, dict) else tuple(arg) if isinstance(arg, list) else arg for arg in args])
            kwargs = {k: frozendict(v) if isinstance(v, dict) else tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()}
            return foo(*args, **kwargs)

        return _wrapped_foo

    return _wrapper


def minutely(maxsize=128, persistent="", custom=None, **kwargs):
    def _wrapper(foo):
        return interval(seconds=60, maxsize=maxsize, persistent=persistent, custom=custom, **kwargs)(foo)

    return _wrapper


def hourly(maxsize=128, persistent="", custom=None, **kwargs):
    def _wrapper(foo):
        return interval(minutes=60, maxsize=maxsize, persistent=persistent, custom=custom, **kwargs)(foo)

    return _wrapper


def daily(maxsize=128, persistent="", custom=None, **kwargs):
    def _wrapper(foo):
        return interval(hours=24, maxsize=maxsize, persistent=persistent, custom=custom, **kwargs)(foo)

    return _wrapper


def monthly(maxsize=128, persistent="", custom=None, **kwargs):
    def _wrapper(foo):
        return interval(months=1, maxsize=maxsize, persistent=persistent, custom=custom, **kwargs)(foo)

    return _wrapper
