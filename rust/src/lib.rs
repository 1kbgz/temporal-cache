use foyer::{
    BlockEngineBuilder, Compression, DeviceBuilder, FsDeviceBuilder,
    HybridCache as FoyerHybridCache, HybridCacheBuilder, HybridCachePolicy, Result as FoyerResult, Scope,
};
use tempfile::{tempdir as gettempdir, TempDir};


#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheOptions {
    Memory(MemoryCacheOptions),
    Disk(DiskCacheOptions),
    Hybrid(HybridCacheOptions),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryCacheOptions {
    pub capacity: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskCacheOptions {
    pub path: String,
    pub capacity: usize,
    pub compress: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridCacheOptions {
    pub memory: MemoryCacheOptions,
    pub disk: DiskCacheOptions,
}

impl CacheOptions {
    pub fn memory(capacity: usize) -> Self {
        CacheOptions::Memory(MemoryCacheOptions { capacity })
    }

    pub fn _disk(
        path: Option<String>,
        capacity: usize,
        compress: bool,
        _gettempdir: Option<fn() -> Result<TempDir, std::io::Error>>,
    ) -> DiskCacheOptions {
        // default cache is /tmp/cache_{geteuid()}
        let default_cache_dir: String = format!("/tmp/cache_{}", unsafe { libc::geteuid() });
        let gettempdir = _gettempdir.unwrap_or(gettempdir);

        let tempdir: String = match gettempdir() {
            Ok(dir) => dir
                .path()
                .to_str()
                .unwrap_or(&default_cache_dir)
                .to_string(),
            Err(_) => default_cache_dir.clone(),
        };
        DiskCacheOptions {
            path: path.unwrap_or_else(|| tempdir.clone()),
            capacity,
            compress,
        }
    }

    pub fn disk(
        path: Option<String>,
        capacity: usize,
        compress: bool,
        _gettempdir: Option<fn() -> Result<TempDir, std::io::Error>>,
    ) -> Self {
        CacheOptions::Disk(Self::_disk(path, capacity, compress, _gettempdir))
    }

    pub fn hybrid(
        memory_capacity: usize,
        path: Option<String>,
        disk_capacity: usize,
        compress: bool,
        _gettempdir: Option<fn() -> Result<TempDir, std::io::Error>>,
    ) -> Self {
        CacheOptions::Hybrid(HybridCacheOptions {
            memory: MemoryCacheOptions {
                capacity: memory_capacity,
            },
            disk: Self::_disk(path, disk_capacity, compress, _gettempdir),
        })
    }
}

#[derive(Clone, Debug)]
pub struct MemoryCache {
    pub options: MemoryCacheOptions,
    pub cache: FoyerHybridCache<String, String>,
}

#[derive(Clone, Debug)]
pub struct DiskCache {
    pub options: DiskCacheOptions,
    pub cache: FoyerHybridCache<String, String>,
}

#[derive(Clone, Debug)]
pub struct HybridCache {
    pub options: HybridCacheOptions,
    pub cache: FoyerHybridCache<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Cache {
    Memory(MemoryCache),
    Disk(DiskCache),
    Hybrid(HybridCache),
}

impl Eq for MemoryCache {}

impl PartialEq for MemoryCache {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options
    }
}

impl Eq for DiskCache {}
impl PartialEq for DiskCache {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options
    }
}

impl Eq for HybridCache {}
impl PartialEq for HybridCache {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options
    }
}

impl MemoryCache {
    pub fn new(options: MemoryCacheOptions) -> Self {
        // Use block_on to await the async cache creation
        let hybrid = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(get_memory_cache(options.clone()))
            .unwrap();

        MemoryCache {
            options: options.clone(),
            cache: hybrid,
        }
    }

    // implement eq for MemoryCache by comparing options
    pub fn eq(&self, other: &MemoryCache) -> bool {
        self.options == other.options
    }
}

async fn get_memory_cache(
    options: MemoryCacheOptions,
) -> FoyerResult<FoyerHybridCache<String, String>> {
    let builder = HybridCacheBuilder::new()
        .with_name("memory-cache")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(options.capacity)
        .storage()
        .build()
        .await?;
    Ok(builder)
}

async fn get_cache(options: DiskCacheOptions) -> FoyerResult<FoyerHybridCache<String, String>> {
    let device = FsDeviceBuilder::new(options.path.clone())
        .with_capacity(options.capacity)
        .build()
        .map_err(|e| foyer::Error::from(Box::new(e) as Box<dyn std::error::Error + Send + Sync>))?;

    let builder = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(0) // No in-memory cache for disk-only cache
        .storage()
        .with_engine_config(
            BlockEngineBuilder::new(device)
                .with_block_size(16 * 1024 * 1024)
        )
        .with_compression(if options.compress {
            Compression::Lz4
        } else {
            Compression::None
        });
    builder.build().await
}

impl DiskCache {
    pub fn new(options: DiskCacheOptions) -> Self {
        // Use block_on to await the async cache creation
        let hybrid = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(get_cache(options.clone()))
            .unwrap();

        DiskCache {
            options: options.clone(),
            cache: hybrid,
        }
    }

    // implement eq for DiskCache by comparing options
    pub fn eq(&self, other: &DiskCache) -> bool {
        self.options == other.options
    }
}

/**********************************/
#[cfg(test)]
mod cache_tests {
    use tempfile::TempDir;
    #[allow(non_upper_case_globals)]
    const _gettempdir: fn() -> Result<TempDir, std::io::Error> =
        || -> Result<TempDir, std::io::Error> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "mocked error",
            ))
        };

    // Lint for constant naming is intentionally ignored for GETTEMPDIR.
    #[test]
    fn test_new_memory() {
        use super::*;
        let options: CacheOptions = CacheOptions::memory(1024);
        let cache: MemoryCache = match options {
            CacheOptions::Memory(mem_opts) => MemoryCache::new(mem_opts),
            _ => panic!("Expected Memory cache options"),
        };
        assert_eq!(cache.options.capacity, 1024);
        cache
            .cache
            .insert(String::from("test"), String::from("test_value"));
        let binding = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(cache.cache.get(&String::from("test")))
            .unwrap()
            .unwrap();
        let value = binding.value();
        assert_eq!(value, "test_value");
    }

    #[test]
    fn test_new_disk_with_path() {
        use super::*;
        let options: CacheOptions =
            CacheOptions::disk(Some("test_cache".to_string()), 2048, true, None);
        let cache: DiskCache = match options {
            CacheOptions::Disk(disk_opts) => DiskCache::new(disk_opts),
            _ => panic!("Expected Disk cache options"),
        };
        assert_eq!(cache.options.path, "test_cache");
        assert_eq!(cache.options.capacity, 2048);
        cache.cache.insert(String::from("test"), String::from("test_value"));
        let binding = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(cache.cache.get(&String::from("test")))
            .unwrap()
            .unwrap();
        let value = binding.value();
        assert_eq!(value, "test_value");
        // create a list of 1000 numbers, convert to string, and insert into cache
        for i in 0..10000 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache.cache.insert(key.clone(), value.clone());
            cache.cache.writer(key.clone()).insert(value.clone());
            let binding = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(cache.cache.get(&key))
                .unwrap()
                .unwrap();
            let cached_value = binding.value();
            assert_eq!(*cached_value, value);
        }
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(cache.cache.memory().flush());
    }

    #[test]
    fn test_new_disk_without_path() {
        use super::*;
        let options: CacheOptions = CacheOptions::disk(None, 2048, true, Some(_gettempdir));

        let cache: DiskCache = match options {
            CacheOptions::Disk(disk_opts) => DiskCache::new(disk_opts),
            _ => panic!("Expected Disk cache options"),
        };
        let expected_default_path = format!("/tmp/cache_{}", unsafe { libc::geteuid() });
        assert_eq!(cache.options.path, expected_default_path);
        assert_eq!(cache.options.capacity, 2048);
        cache.cache.insert(String::from("test"), String::from("test_value"));
        let binding = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(cache.cache.get(&String::from("test")))
            .unwrap()
            .unwrap();
        let value = binding.value();
        assert_eq!(value, "test_value");

        // create a list of 1000 numbers, convert to string, and insert into cache
        for i in 0..10000 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            cache.cache.insert(key.clone(), value.clone());
            cache.cache.writer(key.clone()).insert(value.clone());
            let binding = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(cache.cache.get(&key))
                .unwrap()
                .unwrap();
            let cached_value = binding.value();
            assert_eq!(*cached_value, value);
        }
        cache.cache.memory().flush();
    }

}
