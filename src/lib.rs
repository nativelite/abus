//! **abus**: the coordination layer for a team of agents, on the nativelite
//! stack alone (`json` + std; zero third-party dependencies).
//!
//! Two halves, deliberately separate concerns that a coordinating host (e.g. a
//! multi-agent terminal) composes:
//!
//! * [`board`]: the **durable source of truth**, a schemaless `key → fields`
//!   tracker ("what is currently true": `auth: DONE, owner: Max, url: …`). Read
//!   on demand instead of re-scraping a transcript.
//! * [`bus`]: the **active event stream**, topic-routed pub/sub with structured
//!   events in two urgency classes ([`bus::Kind::Fyi`] vs
//!   [`bus::Kind::DecisionNeeded`]) and backpressure built in ("what just
//!   happened, and does anyone need to act?").
//!
//! Both are **pure logic** over in-memory state with an optional atomic snapshot
//! file: no I/O loop, no sockets, no locking. The design assumes a **single
//! broker process** owns each instance (every participant talks to it over one
//! channel), so a plain map / ring behind that channel needs no consensus. A host
//! wires these to its own IPC and renders them; abus stays transport-agnostic and
//! unit-testable without a terminal.
//!
//! Extracted from atrium (the multi-agent terminal that first grew this layer) so
//! the coordination concern is one package, reusable by any agent host.

pub mod board;
pub mod bus;
