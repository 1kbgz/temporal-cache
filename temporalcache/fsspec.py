# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import fnmatch
import re
from functools import wraps
from typing import Any, Dict, Optional

from . import utils
from .persistent_lru_cache import persistent_lru_cache


class CachedFileSystem:
    r"""A wrapper around fsspec filesystems that provides configurable temporal caching.

    This class wraps any fsspec-compatible filesystem and applies temporal caching
    to filesystem operations based on configurable path patterns.

    Args:
        protocol: The fsspec protocol/filesystem to wrap (e.g., "s3", "gcs", "file")
        cache_config: Dictionary mapping patterns to cache parameters:
            - "paths": Dict of explicit paths to cache params
            - "globs": Dict of glob patterns to cache params
            - "regex": Dict of regex patterns to cache params
            - "default": Default cache params for unmatched paths
        **kwargs: Additional arguments passed to the underlying filesystem

    Example:
        >>> from temporalcache.fsspec import CachedFileSystem
        >>> fs = CachedFileSystem(
        ...     "s3",
        ...     cache_config={
        ...         "paths": {"s3://bucket/config.json": {"hours": 24}},
        ...         "globs": {"*.parquet": {"hours": 1}},
        ...         "regex": {r".*\.tmp$": {"seconds": 30}},
        ...         "default": {"hours": 1},
        ...     },
        ...     anon=False,
        ... )
        >>> with fs.open("s3://bucket/data.parquet") as f:
        ...     data = f.read()  # Cached for 1 hour
    """

    def __init__(self, protocol: str, cache_config: Optional[Dict[str, Any]] = None, **kwargs):
        try:
            import fsspec
        except ImportError:
            raise ImportError("fsspec is required to use CachedFileSystem. Install it with: pip install fsspec")

        self._protocol = protocol
        self._fs = fsspec.filesystem(protocol, **kwargs)
        self._cache_config = cache_config or {}

        # Parse cache configuration
        self._explicit_paths = self._cache_config.get("paths", {})
        self._glob_patterns = self._cache_config.get("globs", {})
        self._regex_patterns = {re.compile(pattern): params for pattern, params in self._cache_config.get("regex", {}).items()}
        self._default_params = self._cache_config.get("default", {})

        # Store cached method functions for clearing
        self._cached_methods = {}

        # Cache wrapped methods
        self._setup_cached_methods()

    def _get_cache_params(self, path: str) -> Dict[str, Any]:
        """Get cache parameters for a given path based on matching rules.

        Priority order:
        1. Explicit path match
        2. Glob pattern match (first match)
        3. Regex pattern match (first match)
        4. Default parameters
        """
        # Check explicit paths first
        if path in self._explicit_paths:
            return self._explicit_paths[path]

        # Check glob patterns
        for pattern, params in self._glob_patterns.items():
            if fnmatch.fnmatch(path, pattern):
                return params

        # Check regex patterns
        for pattern, params in self._regex_patterns.items():
            if pattern.search(path):
                return params

        # Return default parameters
        return self._default_params

    def _create_cached_method(self, method_name: str, original_method):
        """Create a cached version of a filesystem method.

        This wraps the method to apply temporal caching based on the path argument.
        We create a single cached function per (method, cache_params) combination.
        """
        # Store cached functions per method and cache params
        # This will be accessible via closure
        cached_funcs = {}
        # Store the actual lru_cache/persistent_lru_cache instances for clearing
        cached_func_instances = {}

        @wraps(original_method)
        def wrapper(path, *args, **kwargs):
            # Get cache parameters for this path
            cache_params = self._get_cache_params(path)

            # If no cache params, call original method directly
            if not cache_params or utils.TEMPORAL_CACHE_GLOBAL_DISABLE:
                return original_method(path, *args, **kwargs)

            # Create a hashable key from cache params
            cache_key = tuple(sorted(cache_params.items()))

            # Create cached version if it doesn't exist for these params
            if cache_key not in cached_funcs:
                # Extract cache parameters
                maxsize = cache_params.get("maxsize", 128)
                persistent = cache_params.get("persistent", "")

                # Get time parameters
                time_params = {k: v for k, v in cache_params.items() if k in ("seconds", "minutes", "hours", "days", "weeks", "months", "years")}

                # Create the base cached function (without interval wrapper)
                # This allows us to access cache_clear
                if persistent:
                    base_cached = persistent_lru_cache(persistent, maxsize=maxsize)(lambda p, *a, **kw: original_method(p, *a, **kw))
                else:
                    from functools import lru_cache

                    base_cached = lru_cache(maxsize=maxsize)(lambda p, *a, **kw: original_method(p, *a, **kw))

                # Now wrap with interval for time-based expiration
                # We create our own time-checking wrapper instead of using @interval
                # to maintain access to the underlying cache
                import datetime

                from .utils import calc

                last_check = [datetime.datetime.now()]  # Use list for mutability in closure

                def cached_call(p, *a, **kw):
                    # Check global disable flag on each call
                    if utils.TEMPORAL_CACHE_GLOBAL_DISABLE:
                        base_cached.cache_clear()
                        return original_method(p, *a, **kw)

                    now = datetime.datetime.now()
                    if time_params and (now - last_check[0]).total_seconds() > calc(**time_params):
                        base_cached.cache_clear()
                        last_check[0] = now
                    return base_cached(p, *a, **kw)

                cached_funcs[cache_key] = cached_call
                cached_func_instances[cache_key] = base_cached

            # Call the cached version
            return cached_funcs[cache_key](path, *args, **kwargs)

        # Attach the cached_funcs dict so we can access it for clearing
        wrapper._cached_funcs = cached_funcs
        wrapper._cached_func_instances = cached_func_instances
        return wrapper

    def _setup_cached_methods(self):
        """Setup cached versions of key filesystem methods."""
        # Methods to cache (that take path as first argument)
        cacheable_methods = ["cat_file", "cat", "info", "ls", "exists", "size", "checksum", "ukey", "isdir", "isfile"]

        for method_name in cacheable_methods:
            if hasattr(self._fs, method_name):
                original_method = getattr(self._fs, method_name)
                cached_method = self._create_cached_method(method_name, original_method)
                setattr(self, method_name, cached_method)
                # Store reference for cache clearing
                self._cached_methods[method_name] = cached_method

    def open(self, path, mode="rb", **kwargs):
        """Open a file for reading or writing.

        For read operations, this caches the file contents based on path matching rules.
        Write operations bypass the cache.
        """
        # Only cache read operations
        if "r" in mode and "w" not in mode and "a" not in mode:
            cache_params = self._get_cache_params(path)

            if cache_params and not utils.TEMPORAL_CACHE_GLOBAL_DISABLE:
                # For read operations, read the entire file and cache it
                # Then return a BytesIO/StringIO wrapper
                from io import BytesIO, StringIO

                # Use cat_file to get cached content
                content = self.cat_file(path)

                if "b" in mode:
                    return BytesIO(content)
                else:
                    return StringIO(content.decode("utf-8"))

        # For write operations or when caching is disabled, use original open
        return self._fs.open(path, mode=mode, **kwargs)

    def clear_cache(self, path: Optional[str] = None):
        """Clear cached data.

        Args:
            path: If provided, clear cache for this specific path.
                  If None, clear all caches.
        """
        for method_name, method in self._cached_methods.items():
            if hasattr(method, "_cached_func_instances"):
                cached_func_instances = method._cached_func_instances

                if path is None:
                    # Clear all cached functions for this method
                    for cached_func in cached_func_instances.values():
                        if hasattr(cached_func, "cache_clear"):
                            cached_func.cache_clear()
                else:
                    # Clear cached functions that match this path
                    # We need to check which cache params apply to this path
                    cache_params = self._get_cache_params(path)
                    if cache_params:
                        cache_key = tuple(sorted(cache_params.items()))
                        if cache_key in cached_func_instances:
                            if hasattr(cached_func_instances[cache_key], "cache_clear"):
                                cached_func_instances[cache_key].cache_clear()

    def __getattr__(self, name):
        """Forward any unhandled attributes to the underlying filesystem."""
        return getattr(self._fs, name)

    def __repr__(self):
        return f"CachedFileSystem({self._protocol}, cache_config={self._cache_config})"
