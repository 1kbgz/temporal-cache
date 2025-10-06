use pyo3::prelude::*;

mod example;

pub use example::Example;

#[pymodule]
fn temporalcache(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    // Example
    m.add_class::<Example>().unwrap();
    Ok(())
}
