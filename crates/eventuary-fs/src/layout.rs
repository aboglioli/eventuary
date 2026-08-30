use std::path::{Path, PathBuf};

pub const LOG_SUFFIX: &str = "log";
pub const OFFSET_INDEX_SUFFIX: &str = "index";
pub const TIME_INDEX_SUFFIX: &str = "timeindex";
pub const LOCK_FILE: &str = ".lock";
pub const META_FILE: &str = "meta.json";
pub const CHECKPOINTS_DIR: &str = "checkpoints";

const PARTITION_WIDTH: usize = 5;
const OFFSET_WIDTH: usize = 20;

pub fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push_str(&format!("%{:02X}", ch as u32));
        }
    }
    out
}

pub fn decode_component(value: &str) -> Option<String> {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hi = chars.next()?;
            let lo = chars.next()?;
            let code = u8::from_str_radix(&format!("{hi}{lo}"), 16).ok()?;
            out.push(char::from(code));
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

pub fn partition_dir(root: &Path, partition_id: u32) -> PathBuf {
    root.join(format!("{partition_id:0PARTITION_WIDTH$}"))
}

pub fn segment_base_name(base_offset: u64) -> String {
    format!("{base_offset:0OFFSET_WIDTH$}")
}

pub fn segment_path(partition_dir: &Path, base_offset: u64, suffix: &str) -> PathBuf {
    partition_dir.join(format!("{}.{suffix}", segment_base_name(base_offset)))
}

pub fn lock_path(partition_dir: &Path) -> PathBuf {
    partition_dir.join(LOCK_FILE)
}

pub fn meta_path(root: &Path) -> PathBuf {
    root.join(META_FILE)
}

pub fn checkpoints_dir(root: &Path) -> PathBuf {
    root.join(CHECKPOINTS_DIR)
}

pub fn parse_segment_base(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    if path.extension()?.to_str()? != LOG_SUFFIX {
        return None;
    }
    if stem.len() != OFFSET_WIDTH || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

pub fn parse_partition_id(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    if name.len() != PARTITION_WIDTH || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    name.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_dir_is_zero_padded() {
        let dir = partition_dir(Path::new("/log"), 7);

        assert_eq!(dir, Path::new("/log/00007"));
    }

    #[test]
    fn segment_paths_are_zero_padded_and_suffixed() {
        let dir = Path::new("/log/00000");

        assert_eq!(
            segment_path(dir, 4096, LOG_SUFFIX),
            Path::new("/log/00000/00000000000000004096.log")
        );
        assert_eq!(
            segment_path(dir, 0, OFFSET_INDEX_SUFFIX),
            Path::new("/log/00000/00000000000000000000.index")
        );
    }

    #[test]
    fn segment_names_sort_lexicographically_by_offset() {
        let mut names = vec![
            segment_base_name(10),
            segment_base_name(2),
            segment_base_name(100),
        ];
        names.sort();

        assert_eq!(
            names,
            vec![
                segment_base_name(2),
                segment_base_name(10),
                segment_base_name(100)
            ]
        );
    }

    #[test]
    fn parse_segment_base_accepts_log_files_only() {
        assert_eq!(
            parse_segment_base(Path::new("/l/00000000000000004096.log")),
            Some(4096)
        );
        assert_eq!(
            parse_segment_base(Path::new("/l/00000000000000004096.index")),
            None
        );
        assert_eq!(parse_segment_base(Path::new("/l/4096.log")), None);
        assert_eq!(parse_segment_base(Path::new("/l/notanoffset.log")), None);
    }

    #[test]
    fn parse_partition_id_accepts_padded_dirs_only() {
        assert_eq!(parse_partition_id(Path::new("/l/00013")), Some(13));
        assert_eq!(parse_partition_id(Path::new("/l/13")), None);
        assert_eq!(parse_partition_id(Path::new("/l/checkpoints")), None);
    }
}
