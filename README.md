# temporal-cache

Time based function caching

[![Build Status](https://github.com/1kbgz/temporal-cache/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/temporal-cache/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/temporal-cache/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/temporal-cache)
[![License](https://img.shields.io/github/license/1kbgz/temporal-cache)](https://github.com/1kbgz/temporal-cache)
[![PyPI](https://img.shields.io/pypi/v/temporal-cache.svg)](https://pypi.python.org/pypi/temporal-cache)

## Install

From **pip**:

`pip install temporal-cache`

From **conda**:

`conda install temporal-cache -c conda-forge`

## Why?

I needed something that would automagically refresh at 4:00pm when markets close.

```python3

@expire(hour=16)
def fetchFinancialData():
    ...
```

## Interval Cache

The interval cache expires every `time` interval since its first use

```python3

@interval(seconds=5, minutes=2)
def myfoo():
    '''myfoo's lru_cache will expire 2 minutes, 5 seconds after last use'''
    ...
```


## Expire Cache

The expire cache expires on the time given, in scheduler/cron style.

```python3

@expire(second=5, minute=2)
def myfoo():
    '''myfoo's lru_cache will expire on the second minute, fifth second of every hour, every day, etc'''
    ...
```


## Caveats

Python hashing symantics persist. Dicts will be frozen, lists will be converted to tuples. Users are advised to pre-freeze to avoid issues.

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
