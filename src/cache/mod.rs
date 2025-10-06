use pyo3::prelude::*;

use temporalcache::MemoryCacheOptions as BaseMemoryCacheOptions;


#[pyclass]
pub struct MemoryCacheOptions {
    pub base: BaseMemoryCacheOptions,
}

#[pymethods]
impl MemoryCacheOptions {
    #[new]
    fn py_new() -> PyResult<Self> {
        Ok(
            MemoryCacheOptions {
                base: BaseMemoryCacheOptions {
                    capacity: 1024,
                }
            }
        )

    }

    fn __str__(&self) -> PyResult<String>   {
        Ok(format!("{}", self.base.capacity))
    }

    fn __repr__(&self) -> PyResult<String>   {
        Ok(format!("Example<{}>", self.base.capacity))
    }
}
