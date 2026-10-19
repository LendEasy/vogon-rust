//! Playback accounting and scheduling with injected clocks and host-executed I/O.
//!
//! [`stream::SentenceStream`] turns provider bytes into paced media/mark actions;
//! [`queue::OutboundQueue`] orders transport messages and prioritizes clearing.
//! [`state::PlayoutState`] tracks mark receipts and audible-drain state, while
//! [`ledger::Ledger`] reconstructs the agent text at caller onset. [`delivery`]
//! supplies report data and fallback word-duration estimates; [`sentences`]
//! supplies segmentation. These types hold state for one call/playout, not a
//! shared service or provider connection.
//!
//! The host supplies monotonic seconds, executes actions, acknowledges successful
//! dispatch and feeds mark echoes/clears back into receipt state. Sending audio
//! and receiving its playout confirmation are distinct stages. No module here
//! reads a clock, calls Python/audio libraries or performs network I/O. Text
//! operations use Rust standard Unicode whitespace.
/// Delivery outcome data and fallback duration/heard-prefix estimates.
pub mod delivery;
/// Transport message ordering, including media/mark invalidation on clear.
pub mod queue;
/// Text segmentation and short-fragment merging for sentence-mode TTS.
pub mod sentences;
/// Bounded playout history and caller-onset heard/unheard reconstruction.
pub mod ledger;
/// Incremental audio framing, sentence marks and paced host actions.
pub mod stream;
/// Playback mark receipts, drain clearing and estimated heard/unheard state.
pub mod state;
