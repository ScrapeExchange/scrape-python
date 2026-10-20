//! The one-byte per-ID state: state (bits 0-2), force (bit 3) and
//! reason code (bits 4-7).

/// Persisted lifecycle state of a known ID.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State {
    Queued = 0,
    Scraped = 1,
    Failed = 2,
    Unavailable = 3,
    Removed = 4,
}

/// Every state, indexed by its discriminant.
pub const ALL_STATES: [State; 5] = [
    State::Queued,
    State::Scraped,
    State::Failed,
    State::Unavailable,
    State::Removed,
];

impl State {
    pub fn name(&self) -> &'static str {
        match self {
            State::Queued => "queued",
            State::Scraped => "scraped",
            State::Failed => "failed",
            State::Unavailable => "unavailable",
            State::Removed => "removed",
        }
    }

    pub fn parse(name: &str) -> Option<State> {
        ALL_STATES.into_iter().find(|s| s.name() == name)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, State::Failed | State::Unavailable | State::Removed)
    }

    fn from_bits(b: u8) -> State {
        // Bits 0-2 only ever hold values written by Meta::new; 5-7 can
        // only come from corruption, which file checksums catch.
        ALL_STATES[usize::from(b).min(4)]
    }
}

/// Why a video ended in a terminal state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    None = 0,
    RateLimit = 1,
    Transient = 2,
    Unavailable = 3,
    Premiere = 4,
    BindFailed = 5,
    BotDetection = 6,
    RateLimitWait = 7,
    RetriesExhausted = 8,
    Operator = 9,
    Other = 15,
}

const REASONS: [Reason; 11] = [
    Reason::None,
    Reason::RateLimit,
    Reason::Transient,
    Reason::Unavailable,
    Reason::Premiere,
    Reason::BindFailed,
    Reason::BotDetection,
    Reason::RateLimitWait,
    Reason::RetriesExhausted,
    Reason::Operator,
    Reason::Other,
];

impl Reason {
    pub fn name(&self) -> &'static str {
        match self {
            Reason::None => "none",
            Reason::RateLimit => "rate_limit",
            Reason::Transient => "transient",
            Reason::Unavailable => "unavailable",
            Reason::Premiere => "premiere",
            Reason::BindFailed => "bind_failed",
            Reason::BotDetection => "bot_detection",
            Reason::RateLimitWait => "rate_limit_wait",
            Reason::RetriesExhausted => "retries_exhausted",
            Reason::Operator => "operator",
            Reason::Other => "other",
        }
    }

    /// Unknown names map to [`Reason::Other`].
    pub fn from_name(name: &str) -> Reason {
        REASONS
            .into_iter()
            .find(|r| r.name() == name)
            .unwrap_or(Reason::Other)
    }

    fn from_bits(b: u8) -> Reason {
        REASONS
            .into_iter()
            .find(|r| *r as u8 == b)
            .unwrap_or(Reason::Other)
    }
}

const FORCE_BIT: u8 = 1 << 3;

/// The packed per-ID byte.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct Meta(pub u8);

impl Meta {
    pub fn new(state: State, force: bool, reason: Reason) -> Meta {
        let f: u8 = if force { FORCE_BIT } else { 0 };
        Meta(state as u8 | f | ((reason as u8) << 4))
    }

    pub fn state(self) -> State {
        State::from_bits(self.0 & 0x7)
    }

    pub fn force(self) -> bool {
        self.0 & FORCE_BIT != 0
    }

    pub fn reason(self) -> Reason {
        Reason::from_bits(self.0 >> 4)
    }

    pub fn with_force(self, force: bool) -> Meta {
        Meta::new(self.state(), force, self.reason())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_bits() {
        let m: Meta = Meta::new(State::Failed, true, Reason::BotDetection);
        assert_eq!(m.0, 2 | 8 | (6 << 4));
        assert_eq!(
            (m.state(), m.force(), m.reason()),
            (State::Failed, true, Reason::BotDetection)
        );
        assert_eq!(m.with_force(false).0, 2 | (6 << 4));
    }

    #[test]
    fn reason_names() {
        assert_eq!(Reason::from_name("nonsense"), Reason::Other);
        assert_eq!(
            Reason::from_name("retries_exhausted").name(),
            "retries_exhausted"
        );
        assert_eq!(Reason::from_name("none"), Reason::None);
    }

    #[test]
    fn state_names() {
        for s in [
            State::Queued,
            State::Scraped,
            State::Failed,
            State::Unavailable,
            State::Removed,
        ] {
            assert_eq!(State::parse(s.name()), Some(s));
        }
        assert!(State::Failed.is_terminal());
        assert!(!State::Scraped.is_terminal());
        assert_eq!(State::parse("hot"), None);
    }
}
