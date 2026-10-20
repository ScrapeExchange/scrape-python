//! RESP2: parse client commands, encode replies.

use bytes::{Buf, BytesMut};

pub const MAX_ELEMENTS: usize = 1 << 20;
pub const MAX_BULK: usize = 512 * 1024 * 1024;
const MAX_INLINE: usize = 64 * 1024;

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

/// Parse one command. Returns None (consuming nothing) until a whole
/// frame has arrived.
pub fn parse(
    buf: &mut BytesMut,
) -> Result<Option<Vec<Vec<u8>>>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        return parse_inline(buf);
    }
    let Some((mut pos, head)) = line(buf, 1) else {
        return Ok(None);
    };
    let n: i64 = number(head)?;
    if n < 0 {
        buf.advance(pos);
        return Ok(Some(Vec::new()));
    }
    if n as usize > MAX_ELEMENTS {
        return Err(perr("too many elements"));
    }
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        if pos >= buf.len() {
            return Ok(None);
        }
        if buf[pos] != b'$' {
            return Err(perr("expected bulk string"));
        }
        let Some((after, len_s)) = line(buf, pos + 1) else {
            return Ok(None);
        };
        let len: i64 = number(len_s)?;
        if len < 0 || len as usize > MAX_BULK {
            return Err(perr("invalid bulk length"));
        }
        let end: usize = after + len as usize;
        if buf.len() < end + 2 {
            return Ok(None);
        }
        if &buf[end..end + 2] != b"\r\n" {
            return Err(perr("missing CRLF after bulk"));
        }
        out.push(buf[after..end].to_vec());
        pos = end + 2;
    }
    buf.advance(pos);
    Ok(Some(out))
}

fn parse_inline(
    buf: &mut BytesMut,
) -> Result<Option<Vec<Vec<u8>>>, ProtocolError> {
    let Some(nl) = buf.iter().position(|c| *c == b'\n') else {
        if buf.len() > MAX_INLINE {
            return Err(perr("inline command too long"));
        }
        return Ok(None);
    };
    let raw: &[u8] = &buf[..nl];
    let raw: &[u8] = raw.strip_suffix(b"\r").unwrap_or(raw);
    let out: Vec<Vec<u8>> = raw
        .split(|c| c.is_ascii_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_vec())
        .collect();
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
}
