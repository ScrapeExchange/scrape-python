//! Lossless encoding of platform IDs into fixed-width integer keys.

/// The ID does not match the codec's format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidId;

const ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn digit(c: u8) -> Option<u64> {
    let d: u8 = match c {
        b'A'..=b'Z' => c - b'A',
        b'a'..=b'z' => c - b'a' + 26,
        b'0'..=b'9' => c - b'0' + 52,
        b'-' => 62,
        b'_' => 63,
        _ => return None,
    };
    Some(u64::from(d))
}

/// Encode an 11-character YouTube video ID into a u64.
///
/// The first 10 characters are base64url digits of 6 bits each, most
/// significant first. The last character only takes the 16 values
/// `AEIMQUYcgkosw048` (digit divisible by 4) and fills the low 4 bits.
pub fn encode_yt_video_id(s: &str) -> Result<u64, InvalidId> {
    let b: &[u8] = s.as_bytes();
    if b.len() != 11 {
        return Err(InvalidId);
    }
    let mut v: u64 = 0;
    for &c in &b[..10] {
        v = (v << 6) | digit(c).ok_or(InvalidId)?;
    }
    let last: u64 = digit(b[10]).ok_or(InvalidId)?;
    if !last.is_multiple_of(4) {
        return Err(InvalidId);
    }
    Ok((v << 4) | (last / 4))
}

/// Decode a u64 produced by [`encode_yt_video_id`].
pub fn decode_yt_video_id(v: u64) -> String {
    let mut out: [u8; 11] = [0; 11];
    out[10] = ALPHABET[((v & 0xf) * 4) as usize];
    let mut rest: u64 = v >> 4;
    for i in (0..10).rev() {
        out[i] = ALPHABET[(rest & 0x3f) as usize];
        rest >>= 6;
    }
    String::from_utf8(out.to_vec()).expect("alphabet is ASCII")
}

/// Which ID format a queue uses. Only YouTube video IDs exist in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    YoutubeVideo,
}

impl Codec {
    pub fn parse(name: &str) -> Option<Codec> {
        match name {
            "youtube_video" => Some(Codec::YoutubeVideo),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Codec::YoutubeVideo => "youtube_video",
        }
    }

    pub fn encode(&self, s: &str) -> Result<u64, InvalidId> {
        match self {
            Codec::YoutubeVideo => encode_yt_video_id(s),
        }
    }

    pub fn decode(&self, v: u64) -> String {
        match self {
            Codec::YoutubeVideo => decode_yt_video_id(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_known_id() {
        let v: u64 = encode_yt_video_id("dQw4w9WgXcQ").unwrap();
        assert_eq!(decode_yt_video_id(v), "dQw4w9WgXcQ");
    }

    #[test]
    fn extremes() {
        assert_eq!(encode_yt_video_id("AAAAAAAAAAA").unwrap(), 0);
        assert_eq!(encode_yt_video_id("___________"), Err(InvalidId));
        assert_eq!(encode_yt_video_id("__________8").unwrap(), u64::MAX);
        assert_eq!(decode_yt_video_id(0), "AAAAAAAAAAA");
        assert_eq!(decode_yt_video_id(u64::MAX), "__________8");
    }

    #[test]
    fn rejects_bad_ids() {
        for s in [
            "",
            "dQw4w9WgXc",
            "dQw4w9WgXcQQ",
            "dQw4w9WgXcB",
            "dQw4w9WgXc ",
            "dQw4w9Wg+cQ",
            "dQw4w9WgXcé",
        ] {
            assert_eq!(encode_yt_video_id(s), Err(InvalidId), "{s}");
        }
    }

    #[test]
    fn codec_enum_delegates() {
        let c: Codec = Codec::parse("youtube_video").unwrap();
        assert_eq!(c.decode(c.encode("dQw4w9WgXcQ").unwrap()), "dQw4w9WgXcQ");
        assert!(Codec::parse("tiktok_video").is_none());
    }
}
