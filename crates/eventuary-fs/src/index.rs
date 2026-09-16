use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use eventuary_core::Result;

use crate::error::{corrupt, io_at};

pub(crate) const OFFSET_ENTRY_LEN: usize = 8;
pub(crate) const TIME_ENTRY_LEN: usize = 12;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct OffsetEntry {
    pub(crate) relative_offset: u32,
    pub(crate) position: u32,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct TimeEntry {
    pub(crate) timestamp_ms: i64,
    pub(crate) relative_offset: u32,
}

#[derive(Debug)]
pub(crate) struct OffsetIndex {
    path: PathBuf,
    entries: Vec<OffsetEntry>,
}

impl OffsetIndex {
    pub(crate) fn open(path: PathBuf) -> Result<Self> {
        let entries = read_entries(&path, OFFSET_ENTRY_LEN, |chunk| OffsetEntry {
            relative_offset: u32::from_be_bytes(chunk[0..4].try_into().expect("4 bytes")),
            position: u32::from_be_bytes(chunk[4..8].try_into().expect("4 bytes")),
        })?;
        Ok(Self { path, entries })
    }

    pub(crate) fn entries(&self) -> &[OffsetEntry] {
        &self.entries
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn append(&mut self, entry: OffsetEntry) -> Result<()> {
        if let Some(last) = self.entries.last()
            && entry.relative_offset <= last.relative_offset
        {
            return Err(corrupt(
                &self.path,
                format!(
                    "non-monotonic index entry {} after {}",
                    entry.relative_offset, last.relative_offset
                ),
            ));
        }
        let mut bytes = [0u8; OFFSET_ENTRY_LEN];
        bytes[0..4].copy_from_slice(&entry.relative_offset.to_be_bytes());
        bytes[4..8].copy_from_slice(&entry.position.to_be_bytes());
        append_bytes(&self.path, &bytes)?;
        self.entries.push(entry);
        Ok(())
    }

    pub(crate) fn seek(&self, relative_offset: u32) -> u32 {
        match self
            .entries
            .binary_search_by_key(&relative_offset, |e| e.relative_offset)
        {
            Ok(i) => self.entries[i].position,
            Err(0) => 0,
            Err(i) => self.entries[i - 1].position,
        }
    }

    pub(crate) fn truncate_after_position(&mut self, position: u32, persist: bool) -> Result<()> {
        let keep = self
            .entries
            .iter()
            .filter(|e| e.position < position)
            .count();
        if keep == self.entries.len() {
            return Ok(());
        }
        self.entries.truncate(keep);
        if !persist {
            return Ok(());
        }
        rewrite(&self.path, &self.entries, OFFSET_ENTRY_LEN, |entry, out| {
            out[0..4].copy_from_slice(&entry.relative_offset.to_be_bytes());
            out[4..8].copy_from_slice(&entry.position.to_be_bytes());
        })
    }
}

#[derive(Debug)]
pub(crate) struct TimeIndex {
    path: PathBuf,
    entries: Vec<TimeEntry>,
}

impl TimeIndex {
    pub(crate) fn open(path: PathBuf) -> Result<Self> {
        let entries = read_entries(&path, TIME_ENTRY_LEN, |chunk| TimeEntry {
            timestamp_ms: i64::from_be_bytes(chunk[0..8].try_into().expect("8 bytes")),
            relative_offset: u32::from_be_bytes(chunk[8..12].try_into().expect("4 bytes")),
        })?;
        Ok(Self { path, entries })
    }

    pub(crate) fn entries(&self) -> &[TimeEntry] {
        &self.entries
    }

    pub(crate) fn append(&mut self, entry: TimeEntry) -> Result<()> {
        if let Some(last) = self.entries.last()
            && entry.timestamp_ms < last.timestamp_ms
        {
            return Ok(());
        }
        let mut bytes = [0u8; TIME_ENTRY_LEN];
        bytes[0..8].copy_from_slice(&entry.timestamp_ms.to_be_bytes());
        bytes[8..12].copy_from_slice(&entry.relative_offset.to_be_bytes());
        append_bytes(&self.path, &bytes)?;
        self.entries.push(entry);
        Ok(())
    }

    pub(crate) fn seek(&self, timestamp_ms: i64) -> Option<u32> {
        match self
            .entries
            .binary_search_by_key(&timestamp_ms, |e| e.timestamp_ms)
        {
            Ok(i) => Some(self.entries[i].relative_offset),
            Err(0) => self.entries.first().map(|e| e.relative_offset),
            Err(i) => Some(self.entries[i - 1].relative_offset),
        }
    }

    pub(crate) fn truncate_after_relative_offset(
        &mut self,
        relative_offset: u32,
        persist: bool,
    ) -> Result<()> {
        let keep = self
            .entries
            .iter()
            .filter(|e| e.relative_offset <= relative_offset)
            .count();
        if keep == self.entries.len() {
            return Ok(());
        }
        self.entries.truncate(keep);
        if !persist {
            return Ok(());
        }
        rewrite(&self.path, &self.entries, TIME_ENTRY_LEN, |entry, out| {
            out[0..8].copy_from_slice(&entry.timestamp_ms.to_be_bytes());
            out[8..12].copy_from_slice(&entry.relative_offset.to_be_bytes());
        })
    }
}

fn read_entries<T>(path: &Path, entry_len: usize, decode: impl Fn(&[u8]) -> T) -> Result<Vec<T>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_at("open index", path, e)),
    };
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| io_at("read index", path, e))?;
    let usable = buf.len() - (buf.len() % entry_len);
    Ok(buf[..usable].chunks_exact(entry_len).map(decode).collect())
}

fn append_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| io_at("open index for append", path, e))?;
    file.write_all(bytes)
        .map_err(|e| io_at("append index", path, e))
}

fn rewrite<T>(
    path: &Path,
    entries: &[T],
    entry_len: usize,
    encode: impl Fn(&T, &mut [u8]),
) -> Result<()> {
    let file = File::create(path).map_err(|e| io_at("rewrite index", path, e))?;
    let mut writer = BufWriter::new(file);
    let mut buf = vec![0u8; entry_len];
    for entry in entries {
        encode(entry, &mut buf);
        writer
            .write_all(&buf)
            .map_err(|e| io_at("rewrite index", path, e))?;
    }
    writer.flush().map_err(|e| io_at("flush index", path, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        (dir, path)
    }

    #[test]
    fn missing_file_opens_empty() {
        let (_d, path) = temp("absent.index");

        let index = OffsetIndex::open(path).unwrap();

        assert!(index.is_empty());
        assert_eq!(index.seek(100), 0);
    }

    #[test]
    fn appended_entries_survive_reopen() {
        let (_d, path) = temp("a.index");
        let mut index = OffsetIndex::open(path.clone()).unwrap();
        index
            .append(OffsetEntry {
                relative_offset: 0,
                position: 0,
            })
            .unwrap();
        index
            .append(OffsetEntry {
                relative_offset: 10,
                position: 4096,
            })
            .unwrap();

        let reopened = OffsetIndex::open(path).unwrap();

        assert_eq!(reopened.entries().len(), 2);
        assert_eq!(reopened.entries()[1].position, 4096);
    }

    #[test]
    fn seek_returns_floor_entry_position() {
        let (_d, path) = temp("b.index");
        let mut index = OffsetIndex::open(path).unwrap();
        for (relative_offset, position) in [(0u32, 0u32), (10, 4096), (20, 8192)] {
            index
                .append(OffsetEntry {
                    relative_offset,
                    position,
                })
                .unwrap();
        }

        assert_eq!(index.seek(0), 0);
        assert_eq!(index.seek(9), 0);
        assert_eq!(index.seek(10), 4096);
        assert_eq!(index.seek(15), 4096);
        assert_eq!(index.seek(999), 8192);
    }

    #[test]
    fn append_rejects_non_monotonic_offsets() {
        let (_d, path) = temp("c.index");
        let mut index = OffsetIndex::open(path).unwrap();
        index
            .append(OffsetEntry {
                relative_offset: 5,
                position: 100,
            })
            .unwrap();

        let result = index.append(OffsetEntry {
            relative_offset: 5,
            position: 200,
        });

        assert!(result.is_err());
    }

    #[test]
    fn trailing_partial_entry_is_ignored() {
        let (_d, path) = temp("d.index");
        let mut index = OffsetIndex::open(path.clone()).unwrap();
        index
            .append(OffsetEntry {
                relative_offset: 1,
                position: 64,
            })
            .unwrap();
        append_bytes(&path, &[0xff, 0xff, 0xff]).unwrap();

        let reopened = OffsetIndex::open(path).unwrap();

        assert_eq!(reopened.entries().len(), 1);
    }

    #[test]
    fn truncate_after_position_drops_later_entries_on_disk() {
        let (_d, path) = temp("e.index");
        let mut index = OffsetIndex::open(path.clone()).unwrap();
        for (relative_offset, position) in [(0u32, 0u32), (10, 4096), (20, 8192)] {
            index
                .append(OffsetEntry {
                    relative_offset,
                    position,
                })
                .unwrap();
        }

        index.truncate_after_position(5000, true).unwrap();
        let reopened = OffsetIndex::open(path).unwrap();

        assert_eq!(reopened.entries().len(), 2);
        assert_eq!(reopened.entries().last().unwrap().position, 4096);
    }

    #[test]
    fn time_index_seeks_floor_and_ignores_regressions() {
        let (_d, path) = temp("f.timeindex");
        let mut index = TimeIndex::open(path).unwrap();
        index
            .append(TimeEntry {
                timestamp_ms: 1_000,
                relative_offset: 0,
            })
            .unwrap();
        index
            .append(TimeEntry {
                timestamp_ms: 3_000,
                relative_offset: 10,
            })
            .unwrap();
        index
            .append(TimeEntry {
                timestamp_ms: 2_000,
                relative_offset: 20,
            })
            .unwrap();

        assert_eq!(index.entries().len(), 2);
        assert_eq!(index.seek(500), Some(0));
        assert_eq!(index.seek(1_000), Some(0));
        assert_eq!(index.seek(2_999), Some(0));
        assert_eq!(index.seek(9_999), Some(10));
    }
}
