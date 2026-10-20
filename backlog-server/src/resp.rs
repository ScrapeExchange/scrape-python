//! RESP2: parse client commands, encode replies.

use bytes::{Buf, BytesMut};

/// Size limits for one client frame.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_elements: usize,
    pub max_bulk: usize,
    /// Bytes one frame may occupy in the buffer.
    pub max_frame: usize,
}

/// After AUTH. Real clients send at most ~1000 IDs of 11 bytes.
pub const AUTHED: Limits = Limits {
    max_elements: 100_000,
    max_bulk: 1 << 20,
    max_frame: 16 << 20,
};

/// Before AUTH: only small handshake commands are expected.
pub const PRE_AUTH: Limits = Limits {
    max_elements: 8,
    max_bulk: 1024,
    max_frame: 8 * 1024,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Value>>),
    /// A RESP3 map; sent as a flat array to RESP2 clients.
    Map(Vec<(Value, Value)>),
}

#[derive(Debug, thiserror::Error)]
#[error("protocol error: {0}")]
pub struct ProtocolError(pub String);

fn perr(msg: &str) -> ProtocolError {
    ProtocolError(msg.to_string())
}

/// Index just past the next CRLF at or after *from*, and the line.
fn line(b: &[u8], from: usize) -> Option<(usize, &[u8])> {
    let rel: usize = b[from..].windows(2).position(|w| w == b"\r\n")?;
    Some((from + rel + 2, &b[from..from + rel]))
}

fn number(s: &[u8]) -> Result<i64, ProtocolError> {
    std::str::from_utf8(s)
        .ok()
        .and_then(|t| t.parse::<i64>().ok())
        .ok_or_else(|| perr("invalid length"))
}

/// An array frame being assembled across reads.
struct Partial {
    n: usize,
    /// Offset into the buffer just past the last complete element.
    pos: usize,
    args: Vec<Vec<u8>>,
}

/// Incremental command parser. Keeps the elements already read, so a
/// large frame arriving in many reads is scanned once, not once per
/// read.
#[derive(Default)]
pub struct Parser {
    partial: Option<Partial>,
}

impl Parser {
    /// Elements of the frame in progress parsed so far.
    pub fn parsed_elements(&self) -> usize {
        self.partial.as_ref().map_or(0, |p| p.args.len())
    }

    /// Parse one command. Returns None (consuming nothing from *buf*)
    /// until the whole frame has arrived; errors when it would exceed
    /// *lim*.
    pub fn parse(
        &mut self,
        buf: &mut BytesMut,
        lim: &Limits,
    ) -> Result<Option<Vec<Vec<u8>>>, ProtocolError> {
        if self.partial.is_none() {
            if buf.is_empty() {
                return Ok(None);
            }
            if buf[0] != b'*' {
                return parse_inline(buf, lim);
            }
            let Some((pos, head)) = line(buf, 1) else {
                return too_big(buf, lim);
            };
            let n: i64 = number(head)?;
            if n < 0 {
                buf.advance(pos);
                return Ok(Some(Vec::new()));
            }
            if n as usize > lim.max_elements {
                return Err(perr("too many elements"));
            }
            self.partial = Some(Partial {
                n: n as usize,
                pos,
                args: Vec::with_capacity((n as usize).min(1024)),
            });
        }
        let p: &mut Partial = self.partial.as_mut().expect("set above");
        while p.args.len() < p.n {
            if p.pos >= buf.len() {
                break;
            }
            if buf[p.pos] != b'$' {
                self.partial = None;
                return Err(perr("expected bulk string"));
            }
            let Some((after, len_s)) = line(buf, p.pos + 1) else {
                break;
            };
            let len: i64 = number(len_s)?;
            if len < 0 || len as usize > lim.max_bulk {
                self.partial = None;
                return Err(perr("invalid bulk length"));
            }
            let end: usize = after + len as usize;
            if buf.len() < end + 2 {
                break;
            }
            if &buf[end..end + 2] != b"\r\n" {
                self.partial = None;
                return Err(perr("missing CRLF after bulk"));
            }
            p.args.push(buf[after..end].to_vec());
            p.pos = end + 2;
        }
        if p.args.len() == p.n {
            let done: Partial = self.partial.take().expect("set above");
            if done.pos > lim.max_frame {
                return Err(perr("frame too large"));
            }
            buf.advance(done.pos);
            return Ok(Some(done.args));
        }
        if buf.len() > lim.max_frame {
            self.partial = None;
            return Err(perr("frame too large"));
        }
        Ok(None)
    }
}

fn too_big(
    buf: &BytesMut,
    lim: &Limits,
) -> Result<Option<Vec<Vec<u8>>>, ProtocolError> {
    if buf.len() > lim.max_frame {
        return Err(perr("frame too large"));
    }
    Ok(None)
}

/// Parse one command with the post-AUTH limits and no carried state.
pub fn parse(
    buf: &mut BytesMut,
) -> Result<Option<Vec<Vec<u8>>>, ProtocolError> {
    Parser::default().parse(buf, &AUTHED)
}

fn parse_inline(
    buf: &mut BytesMut,
    lim: &Limits,
) -> Result<Option<Vec<Vec<u8>>>, ProtocolError> {
    let Some(nl) = buf.iter().position(|c| *c == b'\n') else {
        return too_big(buf, lim);
    };
    if nl > lim.max_frame {
        return Err(perr("inline command too long"));
    }
    let raw: &[u8] = &buf[..nl];
    let raw: &[u8] = raw.strip_suffix(b"\r").unwrap_or(raw);
    let out: Vec<Vec<u8>> = raw
        .split(|c| c.is_ascii_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
        .collect();
    if out.len() > lim.max_elements {
        return Err(perr("too many elements"));
    }
    buf.advance(nl + 1);
    Ok(Some(out))
}

/// Encode for a RESP2 client.
pub fn encode(v: &Value, out: &mut Vec<u8>) {
    encode_proto(v, out, 2);
}

fn push_len(out: &mut Vec<u8>, prefix: u8, n: usize) {
    out.push(prefix);
    out.extend_from_slice(n.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Encode for a client speaking protocol *proto* (2 or 3). The only
/// differences used here are nulls and maps.
pub fn encode_proto(v: &Value, out: &mut Vec<u8>, proto: u8) {
    match v {
        Value::Simple(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
        }
        Value::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
        }
        Value::Int(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
        }
        Value::Bulk(None) | Value::Array(None) if proto >= 3 => {
            out.push(b'_');
        }
        Value::Bulk(None) => out.extend_from_slice(b"$-1"),
        Value::Array(None) => out.extend_from_slice(b"*-1"),
        Value::Bulk(Some(b)) => {
            push_len(out, b'$', b.len());
            out.extend_from_slice(b);
        }
        Value::Array(Some(items)) => {
            push_len(out, b'*', items.len());
            for item in items {
                encode_proto(item, out, proto);
            }
            return;
        }
        Value::Map(pairs) => {
            if proto >= 3 {
                push_len(out, b'%', pairs.len());
            } else {
                push_len(out, b'*', pairs.len() * 2);
            }
            for (k, val) in pairs {
                encode_proto(k, out, proto);
                encode_proto(val, out, proto);
            }
            return;
        }
    }
    out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<Vec<u8>> {
        v.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn parse_partial_then_complete() {
        let mut b: BytesMut =
            BytesMut::from(&b"*2\r\n$4\r\nPING\r\n$2\r\nh"[..]);
        assert_eq!(parse(&mut b).unwrap(), None);
        assert_eq!(b.len(), 19, "nothing consumed");
        b.extend_from_slice(b"i\r\n*1\r\n$4\r\nPING\r\n");
        assert_eq!(parse(&mut b).unwrap(), Some(args(&["PING", "hi"])));
        assert_eq!(parse(&mut b).unwrap(), Some(args(&["PING"])));
        assert_eq!(parse(&mut b).unwrap(), None);
        assert!(b.is_empty());
    }

    #[test]
    fn parse_inline() {
        let mut b: BytesMut = BytesMut::from(&b"PING  hello\r\nINFO\n"[..]);
        assert_eq!(parse(&mut b).unwrap(), Some(args(&["PING", "hello"])));
        assert_eq!(parse(&mut b).unwrap(), Some(args(&["INFO"])));
    }

    #[test]
    fn parse_rejects_oversize() {
        let mut b: BytesMut = BytesMut::from(&b"*99999999\r\n"[..]);
        assert!(parse(&mut b).is_err());
        let mut b: BytesMut = BytesMut::from(&b"*1\r\n$999999999999\r\n"[..]);
        assert!(parse(&mut b).is_err());
        let mut b: BytesMut = BytesMut::from(&b"*1\r\n+x\r\n"[..]);
        assert!(parse(&mut b).is_err());
    }

    #[test]
    fn encode_resp3_nulls_and_maps() {
        let v: Value = Value::Array(Some(vec![
            Value::Bulk(None),
            Value::Map(vec![(Value::Simple("proto".into()), Value::Int(3))]),
        ]));
        let mut out: Vec<u8> = Vec::new();
        encode_proto(&v, &mut out, 3);
        assert_eq!(out, b"*2\r\n_\r\n%1\r\n+proto\r\n:3\r\n");
        let mut out2: Vec<u8> = Vec::new();
        encode_proto(&v, &mut out2, 2);
        assert_eq!(out2, b"*2\r\n$-1\r\n*2\r\n+proto\r\n:3\r\n");
    }

    #[test]
    fn encode_nested_array_with_nil() {
        let v: Value = Value::Array(Some(vec![
            Value::Bulk(Some(b"id".to_vec())),
            Value::Int(-1),
            Value::Bulk(None),
            Value::Array(Some(vec![Value::Simple("OK".into())])),
            Value::Error("ERR x".into()),
        ]));
        let mut out: Vec<u8> = Vec::new();
        encode(&v, &mut out);
        assert_eq!(
            out,
            b"*5\r\n$2\r\nid\r\n:-1\r\n$-1\r\n*1\r\n+OK\r\n-ERR x\r\n"
        );
    }

    fn frame(n: usize) -> Vec<u8> {
        let mut f: Vec<u8> = format!("*{n}\r\n").into_bytes();
        for i in 0..n {
            let a: String = format!("{i:011}");
            f.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        f
    }

    #[test]
    fn parser_is_incremental() {
        let f: Vec<u8> = frame(50_000);
        let mut p: Parser = Parser::default();
        let mut b: BytesMut = BytesMut::new();
        let mut got: Option<Vec<Vec<u8>>> = None;
        for chunk in f.chunks(4096) {
            b.extend_from_slice(chunk);
            if let Some(v) = p.parse(&mut b, &AUTHED).unwrap() {
                got = Some(v);
            } else {
                assert!(p.parsed_elements() > 0 || b.len() < 64);
            }
        }
        let got: Vec<Vec<u8>> = got.expect("frame completes");
        assert_eq!(got.len(), 50_000);
        assert_eq!(got[49_999], b"00000049999".to_vec());
        assert!(b.is_empty());
    }

    #[test]
    fn pre_auth_limits_are_tight() {
        for raw in [&b"*1\r\n$2000\r\n"[..], b"*100\r\n"] {
            let mut b: BytesMut = BytesMut::from(raw);
            let mut p: Parser = Parser::default();
            assert!(p.parse(&mut b, &PRE_AUTH).is_err(), "{raw:?}");
        }
        let mut b: BytesMut =
            BytesMut::from(&b"*2\r\n$4\r\nAUTH\r\n$2\r\npw\r\n"[..]);
        assert!(
            Parser::default()
                .parse(&mut b, &PRE_AUTH)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn frame_size_is_capped() {
        let lim: Limits = Limits {
            max_frame: 1000,
            ..AUTHED
        };
        let mut b: BytesMut = BytesMut::from(&frame(100)[..1500]);
        assert!(Parser::default().parse(&mut b, &lim).is_err());
        let mut b: BytesMut = BytesMut::from(&vec![b'x'; 1500][..]);
        assert!(Parser::default().parse(&mut b, &lim).is_err());
    }
}
