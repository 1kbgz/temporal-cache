# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import datetime


class TestUtils:
    def test_enable_disable(self):
        self._olddatetime = datetime.datetime

        class NewDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                ret = self._now + self._delay
                print(self._now)
                print(ret)
                return ret

        datetime.datetime = NewDateTime

        from random import random

        from temporalcache import disable, enable, expire

        self._now = datetime.datetime(2018, 1, 1, 1, 1, 0)  # noqa: DTZ001
        self._delay = datetime.timedelta(seconds=0)

        @expire(1)
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        disable()
        print("checking not cached")
        assert x != foo()
        enable()
        x = foo()
        print("checking cached")
        assert x == foo()

        # expire
        self._delay = datetime.timedelta(seconds=2)
        print("checking cache expired")
        assert x != foo()
        print("success")

        datetime.datetime = self._olddatetime

    def test_calc(self):
        from temporalcache.utils import calc

        assert calc(7, 6, 5, 4, 3, 2, 1) == 70002367

    def test_should_expire_seconds(self):
        from temporalcache.utils import should_expire

        now = datetime.datetime(2018, 1, 1, 1, 1, 0)  # noqa: DTZ001
        next_second = now + datetime.timedelta(seconds=1)
        next_secondp = now + datetime.timedelta(seconds=2)
        assert should_expire(now, now, 2) is False
        assert should_expire(now, next_second, 2) is False
        assert should_expire(now, next_secondp, 2)

    def test_should_expire_seconds2(self):
        from temporalcache.utils import should_expire

        now = datetime.datetime(2018, 1, 1, 1, 1, 2)  # noqa: DTZ001
        next_second = now + datetime.timedelta(seconds=7)
        next_second2 = now + datetime.timedelta(seconds=58)
        next_secondp = now + datetime.timedelta(seconds=59)
        assert should_expire(now, now, 1) is False
        assert should_expire(now, next_second, 1) is False
        assert should_expire(now, next_second2, 1) is False
        assert should_expire(now, next_secondp, 1)

    def test_should_expire_minutes(self):
        from temporalcache.utils import should_expire

        now = datetime.datetime(2018, 1, 1, 1, 0, 1)  # noqa: DTZ001
        next_min = now + datetime.timedelta(minutes=1)
        next_minp = now + datetime.timedelta(minutes=2)
        assert should_expire(now, now, 0, 1) is False
        assert should_expire(now, next_min, 0, 1)
        assert should_expire(now, next_minp, 0, 1)

    def test_should_expire_minutes2(self):
        from temporalcache.utils import should_expire

        now = datetime.datetime(2018, 1, 1, 1, 2, 1)  # noqa: DTZ001
        next_min = now + datetime.timedelta(minutes=5)
        next_min2 = now + datetime.timedelta(minutes=58)
        next_minp = now + datetime.timedelta(minutes=59)
        assert should_expire(now, now, 0, 1) is False
        assert should_expire(now, next_min, 0, 1) is False
        assert should_expire(now, next_min2, 0, 1) is False
        assert should_expire(now, next_minp, 0, 1)

    def test_should_expire_seconds_and_minutes(self):
        from temporalcache.utils import should_expire

        now = datetime.datetime(2018, 1, 1, 1, 0, 0)  # noqa: DTZ001
        next_second = now + datetime.timedelta(seconds=1)
        next_min = now + datetime.timedelta(minutes=1)
        next_minp = now + datetime.timedelta(minutes=1, seconds=1)
        assert should_expire(now, now, 1, 1) is False
        assert should_expire(now, next_second, 1, 1) is False
        assert should_expire(now, next_min, 1, 1) is False
        assert should_expire(now, next_minp, 1, 1)

        now = datetime.datetime(2018, 1, 1, 1, 2, 0)  # noqa: DTZ001
        next_min = now + datetime.timedelta(minutes=5)
        next_min2 = now + datetime.timedelta(minutes=58)
        next_minp = now + datetime.timedelta(minutes=59)
        next_minp2 = now + datetime.timedelta(minutes=60, seconds=1)
        assert should_expire(now, now, 1, 1) is False
        assert should_expire(now, next_min, 1, 1) is False
        assert should_expire(now, next_min2, 1, 1) is False
        assert should_expire(now, next_minp, 1, 1)
        assert should_expire(now, next_minp2, 1, 1)

    def test_should_expire_boundary(self):
        from temporalcache.utils import should_expire

        now = datetime.datetime(1999, 12, 31, 23, 59, 58)  # noqa: DTZ001
        next_second = now + datetime.timedelta(seconds=1)
        next_secondp = now + datetime.timedelta(seconds=2)
        assert should_expire(now, now, 0) is False
        assert should_expire(now, next_second, 0) is False
        assert should_expire(now, next_secondp, 0)
