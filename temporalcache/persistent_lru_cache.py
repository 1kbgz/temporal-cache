"""The MIT License (MIT)

Copyright (c) 2013 abarnert

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

"""persistent_lru_cache.py - A persistent LRU cache decorator
"""
# Based on lru_cache from functools in the standard library, which
# was written by Nick Coghlan <ncochlan at gmail.com> and
# Raymond Hettinger <python at rcn.com>. Persistence added by
# Andrew Barnert <abarnert at yahoo.com>.

__all__ = ["persistent_lru_cache"]

try:
    import atexit
    import pickle
    from _thread import RLock
    from collections import namedtuple
    from functools import update_wrapper
except ImportError:

    class RLock:
        "Dummy reentrant lock for builds without threads"

        def __enter__(self):
            pass

        def __exit__(self, exctype, excinst, exctb):
            pass


################################################################################
# LRU Cache function decorator
################################################################################

_CacheInfo = namedtuple("CacheInfo", ["hits", "misses", "maxsize", "currsize"])
_KWD_MARK = (object(),)


class _HashedSeq(list):
    """This class guarantees that hash() will be called no more than once
    per element.  This is important because the lru_cache() will hash
    the key multiple times on a cache miss.

    """

    __slots__ = ("hashvalue",)

    def __init__(self, tup, hash=hash):
        self[:] = tup
        self.hashvalue = hash(tup)

    def __hash__(self):
        return self.hashvalue


def _make_key(
    args,
    kwds,
    typed,
    kwd_mark=_KWD_MARK,
    fasttypes=None,
    sorted=sorted,
    tuple=tuple,
    type=type,
    len=len,
):
    """Make a cache key from optionally typed positional and keyword arguments

    The key is constructed in a way that is flat as possible rather than
    as a nested structure that would take more memory.

    If there is only a single argument and its data type is known to cache
    its hash value, then that argument is returned without a wrapper.  This
    saves space and improves lookup speed.

    """
    if fasttypes is None:
        fasttypes = {int, str, frozenset, type(None)}
    key = args
    if kwds:
        sorted_items = sorted(kwds.items())
        key += kwd_mark
        for item in sorted_items:
            key += item
    if typed:
        key += tuple(type(v) for v in args)
        if kwds:
            key += tuple(type(v) for k, v in sorted_items)
    elif len(key) == 1 and type(key[0]) in fasttypes:
        return key[0]
    return _HashedSeq(key)


def persistent_lru_cache(filename, save_every=1, maxsize=128, typed=False):
    """Least-recently-used cache decorator.

    *filename* is a path to a pickle file that will store the cache
    between runs.

    If *save_every* is set to None, the cache will only be saved at exit
    (or when wrapper.cache_save is explicitly callde). If it's a number N,
    the cache will be saved every N cache misses.

    If *maxsize* is set to None, the LRU features are disabled and the cache
    can grow without bound.

    If *typed* is True, arguments of different types will be cached separately.
    For example, f(3.0) and f(3) will be treated as distinct calls with
    distinct results.

    Arguments to the cached function must be hashable.

    View the cache statistics named tuple (hits, misses, maxsize, currsize)
    with f.cache_info().  Clear the cache and statistics with f.cache_clear().
    Access the underlying function with f.__wrapped__.

    See:  http://en.wikipedia.org/wiki/Cache_algorithms#Least_Recently_Used

    """

    # TODO: Maybe add cache save info to info?

    # Users should only access the lru_cache through its public API:
    #       cache_info, cache_clear, and f.__wrapped__
    # The internals of the lru_cache are encapsulated for thread safety and
    # to allow the implementation to change (including a possible C version).

    # Constants shared by all lru cache instances:
    sentinel = object()  # unique object used to signal cache misses
    make_key = _make_key  # build a key from the function arguments
    PREV, NEXT, KEY, RESULT = 0, 1, 2, 3  # names for the link fields

    def decorating_function(user_function):
        lock = RLock()  # because linkedlist updates aren't threadsafe

        try:
            with open(filename, "rb") as f:
                cache = pickle.load(f)
        except (EOFError, OSError, pickle.PickleError):
            cache = {}

        def cache_save():
            with lock, open(filename, "wb") as f:
                pickle.dump(cache, f)

        atexit.register(cache_save)

        hits = misses = 0
        full = False
        cache_get = cache.get  # bound method to lookup a key or return None
        root = []  # root of the circular doubly linked list
        root[:] = [root, root, None, None]  # initialize by pointing to self

        if maxsize == 0:

            def wrapper(*args, **kwds):
                # No caching -- just a statistics update after a successful call
                nonlocal misses
                result = user_function(*args, **kwds)
                misses += 1
                return result

        elif maxsize is None:

            def wrapper(*args, **kwds):
                # Simple caching without ordering or size limit
                nonlocal hits, misses
                key = make_key(args, kwds, typed)
                result = cache_get(key, sentinel)
                if result is not sentinel:
                    hits += 1
                    return result
                result = user_function(*args, **kwds)
                cache[key] = result
                misses += 1
                if save_every and not misses % save_every:
                    cache_save()
                return result

        else:

            def wrapper(*args, **kwds):
                # Size limited caching that tracks accesses by recency
                nonlocal root, hits, misses, full
                key = make_key(args, kwds, typed)
                with lock:
                    link = cache_get(key)
                    if link is not None:
                        # Move the link to the front of the circular queue
                        link_prev, link_next, _key, result = link
                        link_prev[NEXT] = link_next
                        link_next[PREV] = link_prev
                        last = root[PREV]
                        last[NEXT] = root[PREV] = link
                        link[PREV] = last
                        link[NEXT] = root
                        hits += 1
                        return result
                result = user_function(*args, **kwds)
                with lock:
                    if key in cache:
                        # Getting here means that this same key was added to the
                        # cache while the lock was released.  Since the link
                        # update is already done, we need only return the
                        # computed result and update the count of misses.
                        pass
                    elif full:
                        # Use the old root to store the new key and result.
                        oldroot = root
                        oldroot[KEY] = key
                        oldroot[RESULT] = result
                        # Empty the oldest link and make it the new root.
                        # Keep a reference to the old key and old result to
                        # prevent their ref counts from going to zero during the
                        # update. That will prevent potentially arbitrary object
                        # clean-up code (i.e. __del__) from running while we're
                        # still adjusting the links.
                        root = oldroot[NEXT]
                        oldkey = root[KEY]
                        _ = root[RESULT]
                        root[KEY] = root[RESULT] = None
                        # Now update the cache dictionary.
                        del cache[oldkey]
                        # Save the potentially reentrant cache[key] assignment
                        # for last, after the root and links have been put in
                        # a consistent state.
                        cache[key] = oldroot
                    else:
                        # Put result in a new link at the front of the queue.
                        last = root[PREV]
                        link = [last, root, key, result]
                        last[NEXT] = root[PREV] = cache[key] = link
                        full = len(cache) >= maxsize
                    misses += 1
                    if save_every and not misses % save_every:
                        cache_save()
                return result

        def cache_info():
            """Report cache statistics"""
            with lock:
                return _CacheInfo(hits, misses, maxsize, len(cache))

        def cache_clear():
            """Clear the cache and cache statistics"""
            nonlocal hits, misses, full
            with lock:
                cache.clear()
                root[:] = [root, root, None, None]
                hits = misses = 0
                full = False

        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        wrapper.cache_save = cache_save
        wrapper.cache_filename = filename
        return update_wrapper(wrapper, user_function)

    return decorating_function
