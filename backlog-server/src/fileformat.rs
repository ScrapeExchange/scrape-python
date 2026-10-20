//! The 128-byte header at the start of every data file.
//!
//! Layout (little-endian): magic[4], version u16, kind u8, key_type u8,
//! count u64, generation u64, wal_cut u64, state_counts [u64; 5],
//! payload_crc u32, zero padding, header crc32 of bytes 0..124 at
//! 124..128.

pub const MAGIC: [u8; 4] = *b"BKLG";
pub const FORMAT_VERSION: u16 = 1;
pub const HEADER_LEN: usize = 128;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    BaseIds = 1,
    BaseState = 2,
    Terminal = 3,
    LaneSegment = 4,
    Wal = 5,
}

impl FileKind {
    fn from_u8(v: u8) -> Option<FileKind> {
        [
            FileKind::BaseIds,
            FileKind::BaseState,
            FileKind::Terminal,
            FileKind::LaneSegment,
            FileKind::Wal,
        ]
        .into_iter()
        .find(|k| *k as u8 == v)
    }
}

/// Key width of a queue. Only `U64` is implemented in v1; the others
/// are reserved so files can carry them later without a format bump.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    U64 = 1,
    U128 = 2,
    Bytes = 3,
}

impl KeyType {
    fn from_u8(v: u8) -> Option<KeyType> {
        [KeyType::U64, KeyType::U128, KeyType::Bytes]
            .into_iter()
            .find(|k| *k as u8 == v)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("file too short for header")]
    Short,
    #[error("bad magic")]
    Magic,
    #[error("unsupported format version {0}")]
    Version(u16),
    #[error("header checksum mismatch")]
    Checksum,
    #[error("payload checksum mismatch")]
    PayloadChecksum,
    #[error("unexpected file kind or key type")]
    Kind,
    #[error("inconsistent files: {0}")]
    Inconsistent(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub kind: FileKind,
    pub key_type: KeyType,
    pub count: u64,
    pub generation: u64,
    pub wal_cut: u64,
    pub state_counts: [u64; 5],
    pub payload_crc: u32,
}

impl Header {
    pub fn new(kind: FileKind) -> Header {
        Header {
            kind,
            key_type: KeyType::U64,
            count: 0,
            generation: 0,
            wal_cut: 0,
            state_counts: [0; 5],
            payload_crc: 0,
        }
    }

    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b: [u8; HEADER_LEN] = [0; HEADER_LEN];
        b[0..4].copy_from_slice(&MAGIC);
        b[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        b[6] = self.kind as u8;
        b[7] = self.key_type as u8;
        b[8..16].copy_from_slice(&self.count.to_le_bytes());
        b[16..24].copy_from_slice(&self.generation.to_le_bytes());
        b[24..32].copy_from_slice(&self.wal_cut.to_le_bytes());
        for (i, c) in self.state_counts.iter().enumerate() {
            let o: usize = 32 + i * 8;
            b[o..o + 8].copy_from_slice(&c.to_le_bytes());
        }
        b[72..76].copy_from_slice(&self.payload_crc.to_le_bytes());
        let crc: u32 = crc32fast::hash(&b[..124]);
        b[124..128].copy_from_slice(&crc.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Result<Header, FormatError> {
        if b.len() < HEADER_LEN {
            return Err(FormatError::Short);
        }
        if b[0..4] != MAGIC {
            return Err(FormatError::Magic);
        }
        let stored: u32 = u32_at(b, 124);
        if crc32fast::hash(&b[..124]) != stored {
            return Err(FormatError::Checksum);
        }
        let version: u16 = u16::from_le_bytes([b[4], b[5]]);
        if version != FORMAT_VERSION {
            return Err(FormatError::Version(version));
        }
        let kind: FileKind =
            FileKind::from_u8(b[6]).ok_or(FormatError::Kind)?;
        let key_type: KeyType =
            KeyType::from_u8(b[7]).ok_or(FormatError::Kind)?;
        let mut state_counts: [u64; 5] = [0; 5];
        for (i, c) in state_counts.iter_mut().enumerate() {
            *c = u64_at(b, 32 + i * 8);
        }
        Ok(Header {
            kind,
            key_type,
            count: u64_at(b, 8),
            generation: u64_at(b, 16),
            wal_cut: u64_at(b, 24),
            state_counts,
            payload_crc: u32_at(b, 72),
        })
    }
}

pub fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().expect("8 bytes"))
}

pub fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().expect("4 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Header {
        Header {
            kind: FileKind::BaseIds,
            key_type: KeyType::U64,
            count: 42,
            generation: 7,
            wal_cut: 99,
            state_counts: [1, 2, 3, 4, 5],
            payload_crc: 0xdead_beef,
        }
    }

    #[test]
    fn header_roundtrip() {
        let h: Header = sample();
        let b: [u8; HEADER_LEN] = h.encode();
        assert_eq!(&b[..4], &MAGIC);
        assert_eq!(Header::decode(&b).unwrap(), h);
    }

    #[test]
    fn header_rejects_bad_magic() {
        let mut b: [u8; HEADER_LEN] = sample().encode();
        b[0] = b'X';
        assert!(matches!(Header::decode(&b), Err(FormatError::Magic)));
    }

    #[test]
    fn header_rejects_flipped_bit() {
        let mut b: [u8; HEADER_LEN] = sample().encode();
        b[20] ^= 1;
        assert!(matches!(Header::decode(&b), Err(FormatError::Checksum)));
    }

    #[test]
    fn header_rejects_short_input() {
        assert!(matches!(
            Header::decode(&[0u8; 10]),
            Err(FormatError::Short)
        ));
    }
}
