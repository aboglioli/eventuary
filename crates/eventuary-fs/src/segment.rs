use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use eventuary_core::Result;

use crate::error::{corrupt, io_at};
use crate::index::{OffsetEntry, OffsetIndex, TimeEntry, TimeIndex};
use crate::layout::{LOG_SUFFIX, OFFSET_INDEX_SUFFIX, TIME_INDEX_SUFFIX, segment_path};
use crate::record::Record;

#[derive(Debug, Clone, Copy)]
pub struct SegmentConfig {
    pub index_interval_bytes: u64,
    pub max_bytes: u64,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            index_interval_bytes: 4096,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Segment {
    base_offset: u64,
    next_offset: u64,
    size: u64,
    bytes_since_index: u64,
    log_path: PathBuf,
    offset_index: OffsetIndex,
    time_index: TimeIndex,
    config: SegmentConfig,
    writer: Option<File>,
}

impl Segment {
    pub(crate) fn open(
        dir: &Path,
        base_offset: u64,
        config: SegmentConfig,
        writable: bool,
    ) -> Result<Self> {
        let log_path = segment_path(dir, base_offset, LOG_SUFFIX);
        let mut offset_index =
            OffsetIndex::open(segment_path(dir, base_offset, OFFSET_INDEX_SUFFIX))?;
        let mut time_index = TimeIndex::open(segment_path(dir, base_offset, TIME_INDEX_SUFFIX))?;

        let recovered = recover(&log_path, base_offset, &offset_index)?;
        if writable && recovered.truncated_to < recovered.file_len {
            let file = OpenOptions::new()
                .write(true)
                .open(&log_path)
                .map_err(|e| io_at("open segment for truncate", &log_path, e))?;
            file.set_len(recovered.truncated_to)
                .map_err(|e| io_at("truncate segment", &log_path, e))?;
        }
        if recovered.truncated_to < recovered.indexed_to {
            offset_index.truncate_after_position(recovered.truncated_to as u32, writable)?;
            let last_relative = recovered.next_offset.saturating_sub(base_offset);
            time_index.truncate_after_relative_offset(last_relative as u32, writable)?;
        }

        let writer = if writable {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .map_err(|e| io_at("open segment for append", &log_path, e))?;
            file.seek(SeekFrom::End(0))
                .map_err(|e| io_at("seek segment", &log_path, e))?;
            Some(file)
        } else {
            None
        };

        Ok(Self {
            base_offset,
            next_offset: recovered.next_offset,
            size: recovered.truncated_to,
            bytes_since_index: 0,
            log_path,
            offset_index,
            time_index,
            config,
            writer,
        })
    }

    pub(crate) fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub(crate) fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn has_changed_on_disk(&self) -> Result<bool> {
        match std::fs::metadata(&self.log_path) {
            Ok(meta) => Ok(meta.len() != self.size),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(e) => Err(io_at("stat segment", &self.log_path, e)),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.next_offset == self.base_offset
    }

    pub(crate) fn would_exceed(&self, additional: u64) -> bool {
        !self.is_empty() && self.size + additional > self.config.max_bytes
    }

    pub(crate) fn seek_position(&self, offset: u64) -> u64 {
        if offset <= self.base_offset {
            return 0;
        }
        let relative = (offset - self.base_offset).min(u32::MAX as u64) as u32;
        u64::from(self.offset_index.seek(relative))
    }

    pub(crate) fn seek_offset_for_timestamp(&self, timestamp_ms: i64) -> Option<u64> {
        self.time_index
            .seek(timestamp_ms)
            .map(|relative| self.base_offset + u64::from(relative))
    }

    pub(crate) fn append(&mut self, record: &Record, timestamp_ms: i64) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| corrupt(&self.log_path, "segment opened read-only"))?;
        let encoded = record.encode()?;
        let position = self.size;

        if self.offset_index.is_empty()
            || self.bytes_since_index >= self.config.index_interval_bytes
        {
            let relative = (record.offset - self.base_offset).min(u32::MAX as u64) as u32;
            self.offset_index.append(OffsetEntry {
                relative_offset: relative,
                position: position.min(u32::MAX as u64) as u32,
            })?;
            self.time_index.append(TimeEntry {
                timestamp_ms,
                relative_offset: relative,
            })?;
            self.bytes_since_index = 0;
        }

        writer
            .write_all(&encoded)
            .map_err(|e| io_at("append segment", &self.log_path, e))?;

        self.size += encoded.len() as u64;
        self.bytes_since_index += encoded.len() as u64;
        self.next_offset = record.offset + 1;
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer
                .sync_data()
                .map_err(|e| io_at("sync segment", &self.log_path, e))?;
        }
        Ok(())
    }

    pub(crate) fn read_from(&self, offset: u64, max_records: usize) -> Result<Vec<Record>> {
        if max_records == 0 || offset >= self.next_offset {
            return Ok(Vec::new());
        }
        let file = match File::open(&self.log_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_at("open segment for read", &self.log_path, e)),
        };
        let mut reader = BufReader::new(file);
        let position = self.seek_position(offset);
        reader
            .seek(SeekFrom::Start(position))
            .map_err(|e| io_at("seek segment", &self.log_path, e))?;

        let mut records = Vec::with_capacity(max_records.min(1024));
        let mut consumed = position;
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader
                .read_until(b'\n', &mut line)
                .map_err(|e| io_at("read segment", &self.log_path, e))?;
            if read == 0 || consumed + read as u64 > self.size {
                break;
            }
            consumed += read as u64;
            if line.last() != Some(&b'\n') {
                break;
            }
            let record = Record::decode(&line[..line.len() - 1])?;
            if record.offset >= offset {
                records.push(record);
                if records.len() >= max_records {
                    break;
                }
            }
        }
        Ok(records)
    }

    pub(crate) fn delete(self) -> Result<()> {
        let dir = self
            .log_path
            .parent()
            .ok_or_else(|| corrupt(&self.log_path, "segment has no parent directory"))?
            .to_path_buf();
        drop(self.writer);
        for suffix in [LOG_SUFFIX, OFFSET_INDEX_SUFFIX, TIME_INDEX_SUFFIX] {
            let path = segment_path(&dir, self.base_offset, suffix);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_at("delete segment file", &path, e)),
            }
        }
        Ok(())
    }
}

struct Recovered {
    next_offset: u64,
    truncated_to: u64,
    indexed_to: u64,
    file_len: u64,
}

fn recover(log_path: &Path, base_offset: u64, index: &OffsetIndex) -> Result<Recovered> {
    let file = match File::open(log_path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Recovered {
                next_offset: base_offset,
                truncated_to: 0,
                indexed_to: 0,
                file_len: 0,
            });
        }
        Err(e) => return Err(io_at("open segment", log_path, e)),
    };
    let file_len = file
        .metadata()
        .map_err(|e| io_at("stat segment", log_path, e))?
        .len();

    let (start, expected) = match index.entries().last() {
        Some(entry) => (
            u64::from(entry.position),
            base_offset + u64::from(entry.relative_offset),
        ),
        None => (0, base_offset),
    };
    let mut reader = BufReader::new(file);
    reader
        .seek(SeekFrom::Start(start))
        .map_err(|e| io_at("seek segment", log_path, e))?;

    let scanned = scan(&mut reader, log_path, start, expected)?;

    if scanned.valid_to == start && start > 0 {
        return recover_full_scan(log_path, base_offset, file_len);
    }
    let next_offset = scanned.next_offset;
    let valid_to = scanned.valid_to;

    Ok(Recovered {
        next_offset,
        truncated_to: valid_to,
        indexed_to: file_len,
        file_len,
    })
}

fn recover_full_scan(log_path: &Path, base_offset: u64, file_len: u64) -> Result<Recovered> {
    let file = File::open(log_path).map_err(|e| io_at("open segment", log_path, e))?;
    let mut reader = BufReader::new(file);
    let scanned = scan(&mut reader, log_path, 0, base_offset)?;
    Ok(Recovered {
        next_offset: scanned.next_offset,
        truncated_to: scanned.valid_to,
        indexed_to: file_len,
        file_len,
    })
}

struct Scanned {
    next_offset: u64,
    valid_to: u64,
}

fn scan(
    reader: &mut BufReader<File>,
    log_path: &Path,
    start: u64,
    expected_first: u64,
) -> Result<Scanned> {
    let mut valid_to = start;
    let mut expected = expected_first;
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| io_at("scan segment", log_path, e))?;
        if read == 0 || line.last() != Some(&b'\n') {
            break;
        }
        match Record::decode(&line[..line.len() - 1]) {
            Ok(record) => {
                if record.offset != expected {
                    return Err(corrupt(
                        log_path,
                        format!(
                            "offset {} at byte {valid_to} breaks the dense sequence, expected {expected}; \
                             a second writer has appended to this partition",
                            record.offset
                        ),
                    ));
                }
                valid_to += read as u64;
                expected = record.offset + 1;
            }
            Err(_) => break,
        }
    }
    Ok(Scanned {
        next_offset: expected,
        valid_to,
    })
}
