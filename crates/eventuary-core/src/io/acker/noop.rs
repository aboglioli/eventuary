use crate::error::Result;
use crate::io::Acker;
use crate::io::acker::NackContext;

#[derive(Debug, Clone)]
pub struct NoopAcker;

impl Acker for NoopAcker {
    async fn ack(&self) -> Result<()> {
        Ok(())
    }

    async fn nack(&self) -> Result<()> {
        Ok(())
    }

    async fn nack_with(&self, _context: NackContext) -> Result<()> {
        Ok(())
    }
}
