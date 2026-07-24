# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import datetime
import os
from tempfile import NamedTemporaryFile

if os.name != "nt":

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

            with NamedTemporaryFile() as f:

                @expire(1, persistent=f.name, maxsize=2)
                def foo(test):
                    return random()

                print("running first")
                x = foo("a")
                print("checking cached")
                assert x == foo("a")
                foo("b")
                foo("c")
                x = foo("d")

                # expire
                self._delay = datetime.timedelta(seconds=2)
                print("checking cache expired")
                assert x != foo("d")
                print("success")
