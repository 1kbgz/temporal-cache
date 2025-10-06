use pyo3::prelude::*;

mod cache;

pub use cache::MemoryCacheOptions;


#[pymodule]
fn temporalcache(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Example
    m.add_class::<MemoryCacheOptions>().unwrap();
    Ok(())
}
