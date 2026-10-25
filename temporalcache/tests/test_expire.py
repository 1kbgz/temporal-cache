# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import datetime
import time
from importlib import import_module
from types import SimpleNamespace

import pytest
import pytz


class TestExpire:
    def setup_method(self):
        self._olddatetime = datetime.datetime

        class NewDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                ret = self._now + self._delay
                print(self._now)
                print(ret)
                return ret

        datetime.__dict__["datetime"] = NewDateTime

    def teardown_method(self):
        datetime.__dict__["datetime"] = self._olddatetime

    def test_blank(self):
        from random import random

        from temporalcache import expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 0)
        self._delay = datetime.timedelta(seconds=0)

        @expire(1)
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        # expire
        self._delay = datetime.timedelta(seconds=2)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_mutable(self):
        from random import random

        from temporalcache import expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(seconds=0)

        @expire()
        def foo(*args, **kwargs):
            return random()

        print("running first")
        x = foo([1, 2, 3], test={"a": 1, "b": 2})
        print("checking cached")
        assert x == foo([1, 2, 3], test={"a": 1, "b": 2})

        # expire
        self._delay = datetime.timedelta(seconds=61)
        print("checking cache expired")
        assert x != foo([1, 2, 3], test={"a": 1, "b": 2})
        print("success")

    def test_seconds(self):
        from random import random

        from temporalcache import expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(seconds=0)

        @expire(second=0)
        def foo():
            return random()

        print("running first")
        x = foo()

        print("checking cached")
        self._delay = datetime.timedelta(seconds=58)
        assert x == foo()

        print("checking cached")

        # expire
        self._delay = datetime.timedelta(seconds=59)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_minutes(self):
        from random import random

        from temporalcache import expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(minutes=0)

        @expire(minute=5)
        def foo():
            return random()

        print("running first")
        x = foo()

        print("checking cached")
        self._delay = datetime.timedelta(minutes=3)
        assert x == foo()

        print("checking cached")

        # expire
        self._delay = datetime.timedelta(minutes=4)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_minutely(self):
        from random import random

        from temporalcache import expire_minutely

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(seconds=0)

        @expire_minutely(on=5)
        def foo():
            return random()

        print("running first")
        x = foo()

        print("checking cached")
        self._delay = datetime.timedelta(seconds=3)
        assert x == foo()

        print("checking cached")

        # expire
        self._delay = datetime.timedelta(seconds=4)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_hourly(self):
        from random import random

        from temporalcache import expire_hourly

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(minutes=0)

        @expire_hourly(on=5)
        def foo():
            return random()

        print("running first")
        x = foo()

        print("checking cached")
        self._delay = datetime.timedelta(minutes=3)
        assert x == foo()

        print("checking cached")

        # expire
        self._delay = datetime.timedelta(minutes=4)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_hours(self):
        from random import random

        from temporalcache import expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(hours=0)

        @expire(hour=5)
        def foo():
            return random()

        print("running first")
        x = foo()

        print("checking cached")
        self._delay = datetime.timedelta(hours=3)
        assert x == foo()

        print("checking cached")

        # expire
        self._delay = datetime.timedelta(hours=4)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_daily(self):
        from random import random

        from temporalcache import expire_daily

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(hours=0)

        @expire_daily(on=5)
        def foo():
            return random()

        print("running first")
        x = foo()

        print("checking cached")
        self._delay = datetime.timedelta(hours=3)
        assert x == foo()

        print("checking cached")

        # expire
        self._delay = datetime.timedelta(hours=4)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_days(self):
        from random import random

        from temporalcache import expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)
        self._delay = datetime.timedelta(days=0)

        @expire(day=5)
        def foo():
            return random()

        print("running first")
        x = foo()

        print("checking cached")
        self._delay = datetime.timedelta(days=3)
        assert x == foo()

        print("checking cached")

        # expire
        self._delay = datetime.timedelta(days=4)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_checks1(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(60)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks2(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(minute=60)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks3(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(hour=24)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks4(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(day=0)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks5(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(day=32)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks6(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(week=0)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks7(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(week=6)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks8(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(month=0)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass

    def test_checks9(self):
        from random import random

        from temporalcache import TCException, expire

        try:

            @expire(month=13)
            def foo():
                return random()

            raise AssertionError  # pragma: no cover
        except TCException:
            pass


class TestExpireTZ:
    @pytest.mark.parametrize(("is_dst", "seconds"), [(0, -18000), (1, -14400)])
    def test_local_timezone_fallback(self, monkeypatch, is_dst, seconds):
        module = import_module("temporalcache.expire")

        def unavailable():
            raise AttributeError

        class LocalDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                assert tz is not None
                assert tz.utcoffset(None) == datetime.timedelta(seconds=seconds)
                return cls(2020, 1, 1, tzinfo=tz)

        monkeypatch.setattr(module, "get_localzone", unavailable)
        monkeypatch.setattr(time, "tzname", ("EST", "EDT"))
        monkeypatch.setattr(time, "daylight", 1)
        monkeypatch.setattr(time, "timezone", 18000)
        monkeypatch.setattr(time, "altzone", 14400)
        monkeypatch.setattr(time, "localtime", lambda: SimpleNamespace(tm_isdst=is_dst))
        monkeypatch.setattr(datetime, "datetime", LocalDateTime)

        @module.expire(day=1)
        def cached():
            return object()

        assert cached() is cached()

    def test_unknown_timezone(self):
        from temporalcache import expire

        with pytest.raises(pytz.UnknownTimeZoneError):
            expire(tz="Not/A_Timezone")

    def test_tzexpire(self):
        from random import random

        from temporalcache import expire

        now = datetime.datetime.now(tz=pytz.UTC)

        @expire(second=(now.second + 3) % 60, hour=now.hour, tz="UTC")
        def foo():
            return random()

        ret = foo()
        time.sleep(1)
        assert ret == foo()
        time.sleep(2)
        assert ret == foo()
