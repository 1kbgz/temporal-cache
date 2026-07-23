# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import datetime


class TestInterval:
    def setup_method(self):
        self._olddatetime = datetime.datetime
        _now = datetime.datetime.now()  # noqa: DTZ005

        class NewDateTime(datetime.datetime):
            @classmethod
            def now(cls):
                ret = _now + self._delay
                print(_now)
                print(ret)
                return ret

        datetime.datetime = NewDateTime

    def teardown_method(self):
        datetime.datetime = self._olddatetime

    def test_blank(self):
        from random import random

        from temporalcache import interval

        self._delay = datetime.timedelta(seconds=0)

        @interval()
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

        from temporalcache import interval

        self._delay = datetime.timedelta(seconds=0)

        @interval()
        def foo(*args, **kwargs):
            return random()

        print("running first")
        x = foo([1, 2, 3], test={"a": 1, "b": 2})
        print("checking cached")
        assert x == foo([1, 2, 3], test={"a": 1, "b": 2})

        # expire
        self._delay = datetime.timedelta(seconds=2)
        print("checking cache expired")
        assert x != foo([1, 2, 3], test={"a": 1, "b": 2})
        print("success")

    def test_seconds(self):
        from random import random

        from temporalcache import interval

        self._delay = datetime.timedelta(seconds=0)

        @interval(seconds=1)
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

    def test_minutes(self):
        from random import random

        import temporalcache

        self._delay = datetime.timedelta(seconds=0)

        @temporalcache.interval(minutes=1)
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        self._delay = datetime.timedelta(minutes=1, seconds=1)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_minutely(self):
        from random import random

        import temporalcache

        self._delay = datetime.timedelta(seconds=0)

        @temporalcache.minutely()
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        self._delay = datetime.timedelta(minutes=1, seconds=1)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_hours(self):
        from random import random

        import temporalcache

        self._delay = datetime.timedelta(seconds=0)

        @temporalcache.interval(hours=1)
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        self._delay = datetime.timedelta(minutes=60, seconds=1)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_hourly(self):
        from random import random

        import temporalcache

        self._delay = datetime.timedelta(seconds=0)

        @temporalcache.hourly()
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        self._delay = datetime.timedelta(minutes=60, seconds=1)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_daily(self):
        from random import random

        import temporalcache

        self._delay = datetime.timedelta(seconds=0)

        @temporalcache.daily()
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        self._delay = datetime.timedelta(hours=24, seconds=1)
        print("checking cache expired")
        assert x != foo()
        print("success")

    def test_monthly(self):
        from random import random

        import temporalcache

        self._delay = datetime.timedelta(seconds=0)

        @temporalcache.daily()
        def foo():
            return random()

        print("running first")
        x = foo()
        print("checking cached")
        assert x == foo()

        self._delay = datetime.timedelta(days=31, seconds=1)
        print("checking cache expired")
        assert x != foo()
        print("success")
