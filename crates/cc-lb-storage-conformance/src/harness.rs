pub trait ConformanceBackend: Send + Sync {}

#[derive(Debug)]
pub struct ConformanceFixture<B> {
    backend: B,
}

impl<B> ConformanceFixture<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }
}
