# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import datetime
import time

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

        datetime.datetime = NewDateTime

    def teardown_method(self):
        datetime.datetime = self._olddatetime

    def test_blank(self):
        from random import random

        from temporalcache import expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 0)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 1)  # noqa: DTZ001
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
