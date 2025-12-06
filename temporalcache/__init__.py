# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
from .expire import daily as expire_daily, expire, hourly as expire_hourly, minutely as expire_minutely, monthly as expire_monthly
from .interval import daily, hourly, interval, minutely, monthly
from .utils import (
    TEMPORAL_CACHE_GLOBAL_DISABLE,
    StorageBase,
    TCException,
    disable,
    enable,
)

try:
    from .fsspec import CachedFileSystem
except ImportError:
    # fsspec is optional
    pass

__version__ = "0.1.6"
