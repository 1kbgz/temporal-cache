# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import os
from abc import ABCMeta, abstractmethod
from functools import lru_cache, wraps

TEMPORAL_CACHE_GLOBAL_DISABLE = os.environ.get("TEMPORAL_CACHE_DISABLE", "false")


def disable():
    global TEMPORAL_CACHE_GLOBAL_DISABLE
    TEMPORAL_CACHE_GLOBAL_DISABLE = True


def enable():
    global TEMPORAL_CACHE_GLOBAL_DISABLE
    TEMPORAL_CACHE_GLOBAL_DISABLE = False


class TCException(Exception):
    pass


class StorageBase(metaclass=ABCMeta):
    @abstractmethod
    def cache_clear(self):
        pass

    def __call__(self, foo):
        """No caching by default"""

        @wraps(foo)
        def _wrapper(*args, **kwargs):
            return foo(*args, **kwargs)

        return _wrapper


def _base(last, now, lap, offset, multiple, attr):
    """
    last - last datetime
    now - current datetime
    lap - at what point to "roll over"
    offset - how many seconds between laps
    attr - what to look at
    multiple - what to multiply attr by to get seconds
    """
    # last started before :X, so if now > :X

    # handle windows timestamp issues
    try:
        now_ts = now.timestamp()
    except OSError:
        now_ts = 0
    try:
        last_ts = last.timestamp()
    except OSError:
        last_ts = 0

    diff = round(now_ts, 0) - round(last_ts, 0)
    min_gap = offset - getattr(last, attr) * multiple
    if diff > offset:
        return True
    if getattr(last, attr) < lap:
        return getattr(now, attr) >= lap
    # last started after :X, so if now > last + interval, or if now > :X
    elif getattr(now, attr) >= lap and diff >= min_gap:
        return True
    return False


def _secondly(last, now, secondly):
    return _base(last=last, now=now, lap=secondly, offset=60, multiple=1, attr="second")


def _minutely(last, now, minutely):
    return _base(last=last, now=now, lap=minutely, offset=3600, multiple=60, attr="minute")


def _hourly(last, now, hourly, tz=None):
    return _base(last=last, now=now, lap=hourly, offset=3600 * 24, multiple=3600, attr="hour")


def _daily(last, now, daily, tz=None):
    return _base(
        last=last,
        now=now,
        lap=daily,
        offset=3600 * 24 * 30,
        multiple=3600 * 24,
        attr="day",
    )  # FIXME day should be derived from calendar


def _day_of_week(last, now, day_of_week, tz=None):
    return _base(
        last=last,
        now=now,
        lap=day_of_week,
        offset=3600 * 24 * 7,
        multiple=3600 * 24,
        attr="day",
    )


def _weekly(last, now, weekly, tz=None):
    return _base(
        last=last,
        now=now,
        lap=weekly,
        offset=3600 * 24 * 7 * 4.34,
        multiple=3600 * 24 * 7,
        attr="week",
    )  # FIXME # of weeks should be derived from calendar


def _monthly(last, now, monthly, tz=None):
    return _base(
        last=last,
        now=now,
        lap=monthly,
        offset=3600 * 24 * 365,
        multiple=3600 * 24 * 7 * 4.34,
        attr="month",
    )  # FIXME # of weeks should be derived from calendar


def should_expire(
    last,
    now,
    secondly=None,
    minutely=None,
    hourly=None,
    daily=None,
    day_of_week=None,
    weekly=None,
    monthly=None,
):
    """should the cache expire?
    last - datetime
    now - datetime

    if yearly:
        necessary_distance = calc(0, 0, 0, 0, 0, 0, yearly)
    """
    sec_res = _secondly(last, now, secondly) if secondly is not None else True
    min_res = _minutely(last, now, minutely) if minutely is not None else True
    hou_res = _hourly(last, now, hourly) if hourly is not None else True
    dow_res = _day_of_week(last, now, day_of_week) if day_of_week is not None else True
    dai_res = _daily(last, now, daily) if daily is not None else True
    wee_res = _weekly(last, now, weekly) if weekly is not None else True
    mon_res = _monthly(last, now, monthly) if weekly is not None else True
    return all((sec_res, min_res, hou_res, dai_res, dow_res, wee_res, mon_res))


@lru_cache(1000)
def calc(seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0):
    return (
        seconds
        + minutes * 60
        + hours * 60 * 60
        + days * 24 * 60 * 60
        + weeks * 7 * 24 * 60 * 60
        + months * 30 * 7 * 24 * 60 * 60
        + years * 365 * 24 * 60 * 60
    )
