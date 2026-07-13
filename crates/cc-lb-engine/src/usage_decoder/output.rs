use std::io::{self, Write};

pub(crate) struct BudgetedOutput {
    bytes: Vec<u8>,
    written_bytes: usize,
    budget_bytes: usize,
}

impl BudgetedOutput {
    pub(super) const fn new(budget_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            written_bytes: 0,
            budget_bytes,
        }
    }

    pub(super) fn take_bytes(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.bytes)
    }

    pub(super) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BudgetedOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(next_written_bytes) = self.written_bytes.checked_add(buf.len()) else {
            return Err(decompression_output_budget_error(self.budget_bytes));
        };
        if next_written_bytes > self.budget_bytes {
            return Err(decompression_output_budget_error(self.budget_bytes));
        }
        self.bytes.extend_from_slice(buf);
        self.written_bytes = next_written_bytes;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Sink accessor is passed in because the orphan rule blocks a blanket
// `AsMut<Vec<u8>>` impl on the foreign decoder types from flate2/brotli/zstd.
pub(super) fn write_and_drain<W, F>(
    writer: &mut W,
    input: &[u8],
    mut extract: F,
) -> io::Result<Vec<u8>>
where
    W: Write,
    F: FnMut(&mut W) -> &mut BudgetedOutput,
{
    writer.write_all(input)?;
    writer.flush()?;
    Ok(extract(writer).take_bytes())
}

fn decompression_output_budget_error(budget_bytes: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        format!("decompression output exceeds {budget_bytes}-byte budget"),
    )
}
