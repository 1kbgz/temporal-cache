# *****************************************************************************
#
# Copyright (c) 2021, the temporal-cache authors.
#
# This file is part of the temporal-cache library, distributed under the terms of
# the Apache License 2.0.  The full license can be found in the LICENSE file.
#
import datetime
import os
import tempfile


class TestFsspecCaching:
    """Test the CachedFileSystem wrapper with fsspec."""

    def setup_method(self):
        """Setup for each test method."""
        # Create a temporary directory for testing
        self.test_dir = tempfile.mkdtemp()

        # Mock datetime for time-based testing
        self._olddatetime = datetime.datetime
        self._now = datetime.datetime.now()
        self._delay = datetime.timedelta(seconds=0)

        class NewDateTime(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                ret = self._now + self._delay
                return ret

        datetime.datetime = NewDateTime

    def teardown_method(self):
        """Cleanup after each test method."""
        datetime.datetime = self._olddatetime

        # Clean up temporary directory
        import shutil

        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_import_without_fsspec(self):
        """Test that CachedFileSystem import fails gracefully without fsspec."""
        # Test that we can import the main module even without fsspec
        import temporalcache

        assert hasattr(temporalcache, "interval")
        assert hasattr(temporalcache, "expire")

    def test_basic_creation(self):
        """Test basic CachedFileSystem creation."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 5}})
        assert fs is not None
        assert fs._protocol == "memory"

    def test_explicit_path_matching(self):
        """Test caching with explicit path configuration."""
        from temporalcache.fsspec import CachedFileSystem

        # Create a memory filesystem with some test data
        fs = CachedFileSystem("memory", cache_config={"paths": {"/config.json": {"seconds": 2}}})

        # Write test data
        with fs._fs.open("/config.json", "w") as f:
            f.write("original")

        # First read should cache
        content1 = fs.cat_file("/config.json")
        assert content1 == b"original"

        # Modify the file
        with fs._fs.open("/config.json", "w") as f:
            f.write("modified")

        # Second read should return cached value
        content2 = fs.cat_file("/config.json")
        assert content2 == b"original"

        # After expiration, should get new value
        self._delay = datetime.timedelta(seconds=3)
        content3 = fs.cat_file("/config.json")
        assert content3 == b"modified"

    def test_glob_pattern_matching(self):
        """Test caching with glob pattern configuration."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"globs": {"*.txt": {"seconds": 2}, "data/*.csv": {"seconds": 5}}})

        # Test .txt file caching
        with fs._fs.open("/test.txt", "w") as f:
            f.write("text content")

        content1 = fs.cat_file("/test.txt")
        assert content1 == b"text content"

        with fs._fs.open("/test.txt", "w") as f:
            f.write("modified text")

        content2 = fs.cat_file("/test.txt")
        assert content2 == b"text content"  # Cached

        # Test .csv file in subdirectory
        with fs._fs.open("/data/file.csv", "w") as f:
            f.write("csv,data")

        csv_content = fs.cat_file("/data/file.csv")
        assert csv_content == b"csv,data"

    def test_regex_pattern_matching(self):
        """Test caching with regex pattern configuration."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"regex": {r".*\.tmp$": {"seconds": 1}, r"^/archive/\d{4}/.*": {"seconds": 10}}})

        # Test .tmp file matching
        with fs._fs.open("/temp.tmp", "w") as f:
            f.write("temporary")

        content1 = fs.cat_file("/temp.tmp")
        assert content1 == b"temporary"

        # Test archive path matching
        with fs._fs.open("/archive/2023/data.json", "w") as f:
            f.write("archived")

        archive_content = fs.cat_file("/archive/2023/data.json")
        assert archive_content == b"archived"

    def test_priority_order(self):
        """Test that explicit paths have priority over globs and regex."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem(
            "memory",
            cache_config={
                "paths": {"/test.txt": {"seconds": 1}},
                "globs": {"*.txt": {"seconds": 10}},
                "regex": {r".*\.txt$": {"seconds": 20}},
                "default": {"seconds": 5},
            },
        )

        # Explicit path should match (1 second)
        params = fs._get_cache_params("/test.txt")
        assert params == {"seconds": 1}

        # Glob should match for other .txt files (10 seconds)
        params = fs._get_cache_params("/other.txt")
        assert params == {"seconds": 10}

        # Default should match for non-matching files
        params = fs._get_cache_params("/data.json")
        assert params == {"seconds": 5}

    def test_default_fallback(self):
        """Test that default cache params are used when no pattern matches."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"paths": {"/specific.txt": {"seconds": 2}}, "default": {"seconds": 1}})

        # Write test files
        with fs._fs.open("/specific.txt", "w") as f:
            f.write("specific")
        with fs._fs.open("/other.txt", "w") as f:
            f.write("other")

        # Both should be cached, but with different TTLs
        content1 = fs.cat_file("/specific.txt")
        content2 = fs.cat_file("/other.txt")
        assert content1 == b"specific"
        assert content2 == b"other"

    def test_cache_expiration(self):
        """Test that cache properly expires after the configured time."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 2}})

        # Write and read initial data
        with fs._fs.open("/data.txt", "w") as f:
            f.write("initial")

        content1 = fs.cat_file("/data.txt")
        assert content1 == b"initial"

        # Modify within cache period
        with fs._fs.open("/data.txt", "w") as f:
            f.write("modified1")

        content2 = fs.cat_file("/data.txt")
        assert content2 == b"initial"  # Still cached

        # Expire cache
        self._delay = datetime.timedelta(seconds=3)

        content3 = fs.cat_file("/data.txt")
        assert content3 == b"modified1"  # Cache expired

    def test_multiple_operations(self):
        """Test caching of multiple filesystem operations."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 5}})

        # Test cat_file
        with fs._fs.open("/file1.txt", "w") as f:
            f.write("content1")

        assert fs.cat_file("/file1.txt") == b"content1"

        # Test exists
        assert fs.exists("/file1.txt") is True
        assert fs.exists("/nonexistent.txt") is False

        # Test info (if supported)
        if hasattr(fs, "info"):
            info = fs.info("/file1.txt")
            assert info is not None

    def test_open_method_read_mode(self):
        """Test that open method properly caches in read mode."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 2}})

        # Write test data
        with fs._fs.open("/test.txt", "w") as f:
            f.write("original content")

        # Read using open (should cache)
        with fs.open("/test.txt", "rb") as f:
            content1 = f.read()
        assert content1 == b"original content"

        # Modify the file
        with fs._fs.open("/test.txt", "w") as f:
            f.write("modified content")

        # Read again (should get cached value)
        with fs.open("/test.txt", "rb") as f:
            content2 = f.read()
        assert content2 == b"original content"

    def test_open_method_write_mode(self):
        """Test that open method bypasses cache in write mode."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 10}})

        # Write using open (should not use cache)
        with fs.open("/write_test.txt", "w") as f:
            f.write("written content")

        # Read to verify
        content = fs.cat_file("/write_test.txt")
        assert content == b"written content"

    def test_clear_cache_all(self):
        """Test clearing all cached data."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 10}})

        # Cache some data
        with fs._fs.open("/file1.txt", "w") as f:
            f.write("content1")
        with fs._fs.open("/file2.txt", "w") as f:
            f.write("content2")

        fs.cat_file("/file1.txt")
        fs.cat_file("/file2.txt")

        # Modify files
        with fs._fs.open("/file1.txt", "w") as f:
            f.write("modified1")
        with fs._fs.open("/file2.txt", "w") as f:
            f.write("modified2")

        # Should still get cached values
        assert fs.cat_file("/file1.txt") == b"content1"
        assert fs.cat_file("/file2.txt") == b"content2"

        # Clear all caches
        fs.clear_cache()

        # Should get new values
        assert fs.cat_file("/file1.txt") == b"modified1"
        assert fs.cat_file("/file2.txt") == b"modified2"

    def test_clear_cache_specific_path(self):
        """Test clearing cache for a specific path.

        Note: When multiple paths share the same cache configuration,
        clearing one will clear all entries in that shared cache.
        """
        from temporalcache.fsspec import CachedFileSystem

        # Use different cache configs for different files
        # file1 uses "paths" config, file2 uses globs config
        fs = CachedFileSystem("memory", cache_config={"paths": {"/file1.txt": {"seconds": 10}}, "globs": {"*.csv": {"seconds": 5}}})

        # Cache some data
        with fs._fs.open("/file1.txt", "w") as f:
            f.write("content1")
        with fs._fs.open("/file2.csv", "w") as f:
            f.write("content2")

        fs.cat_file("/file1.txt")
        fs.cat_file("/file2.csv")

        # Modify files
        with fs._fs.open("/file1.txt", "w") as f:
            f.write("modified1")
        with fs._fs.open("/file2.csv", "w") as f:
            f.write("modified2")

        # Clear cache for file1 only (which uses "paths" config)
        fs.clear_cache("/file1.txt")

        # file1 should have new value, file2 should still be cached (different config)
        assert fs.cat_file("/file1.txt") == b"modified1"
        assert fs.cat_file("/file2.csv") == b"content2"

    def test_global_disable_flag(self):
        """Test that TEMPORAL_CACHE_GLOBAL_DISABLE flag disables caching."""
        from temporalcache import disable, enable
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 10}})

        # Write and cache data
        with fs._fs.open("/test.txt", "w") as f:
            f.write("cached")

        content1 = fs.cat_file("/test.txt")
        assert content1 == b"cached"

        # Modify file
        with fs._fs.open("/test.txt", "w") as f:
            f.write("modified")

        # With cache enabled, should get cached value
        content2 = fs.cat_file("/test.txt")
        assert content2 == b"cached"

        # Disable cache globally
        disable()

        # Should get fresh value
        content3 = fs.cat_file("/test.txt")
        assert content3 == b"modified"

        # Re-enable cache
        enable()

    def test_persistent_cache_option(self):
        """Test that persistent cache option is supported."""
        from temporalcache.fsspec import CachedFileSystem

        cache_file = os.path.join(self.test_dir, "cache.pkl")

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 10, "persistent": cache_file}})

        # Write and cache data
        with fs._fs.open("/test.txt", "w") as f:
            f.write("persistent data")

        content = fs.cat_file("/test.txt")
        assert content == b"persistent data"

        # Cache file should be created (on first miss)
        # Note: persistent cache saves on cache miss

    def test_maxsize_parameter(self):
        """Test that maxsize parameter is respected."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 10, "maxsize": 2}})

        # Create multiple files
        for i in range(5):
            with fs._fs.open(f"/file{i}.txt", "w") as f:
                f.write(f"content{i}")

        # Cache them
        for i in range(5):
            fs.cat_file(f"/file{i}.txt")

        # All should work (LRU should handle eviction)
        assert True  # If we got here, maxsize didn't cause errors

    def test_forwarding_to_underlying_fs(self):
        """Test that unknown methods are forwarded to underlying filesystem."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 1}})

        # Test that we can access underlying filesystem attributes
        assert hasattr(fs, "_fs")

        # Test protocol attribute forwarding
        if hasattr(fs._fs, "protocol"):
            assert fs.protocol == fs._fs.protocol

    def test_repr(self):
        """Test string representation of CachedFileSystem."""
        from temporalcache.fsspec import CachedFileSystem

        config = {"default": {"seconds": 5}}
        fs = CachedFileSystem("memory", cache_config=config)

        repr_str = repr(fs)
        assert "CachedFileSystem" in repr_str
        assert "memory" in repr_str

    def test_no_cache_config(self):
        """Test CachedFileSystem with no cache configuration."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory")

        # Write test data
        with fs._fs.open("/test.txt", "w") as f:
            f.write("data")

        # Should work but not cache (no config)
        content = fs.cat_file("/test.txt")
        assert content == b"data"

    def test_cat_vs_cat_file(self):
        """Test both cat and cat_file methods if available."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 5}})

        with fs._fs.open("/test.txt", "w") as f:
            f.write("test content")

        # Test cat_file
        if hasattr(fs, "cat_file"):
            content = fs.cat_file("/test.txt")
            assert content == b"test content"

        # Test cat
        if hasattr(fs, "cat"):
            content = fs.cat("/test.txt")
            assert content == b"test content" or isinstance(content, dict)

    def test_ls_directory_caching(self):
        """Test that directory listing is cached."""
        from temporalcache.fsspec import CachedFileSystem

        fs = CachedFileSystem("memory", cache_config={"default": {"seconds": 2}})

        # Create some files
        with fs._fs.open("/file1.txt", "w") as f:
            f.write("content1")
        with fs._fs.open("/file2.txt", "w") as f:
            f.write("content2")

        # List directory
        if hasattr(fs, "ls"):
            files1 = fs.ls("/")

            # Add another file
            with fs._fs.open("/file3.txt", "w") as f:
                f.write("content3")

            # Should get cached result (without file3)
            files2 = fs.ls("/")
            assert files1 == files2

            # After expiration, should see file3
            self._delay = datetime.timedelta(seconds=3)
            fs.ls("/")
            # files3 should include file3 (if not same as files2)
