# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Deferred persistence: `Bus::with_file_deferred` / `Board::with_file_deferred`
  and `take_pending_write`.** A file-backed bus or board rewrote its whole state
  on every mutation, on the mutating thread — a full 512-event ring is ~0.5 MB and
  several milliseconds per write. A deferred one only marks itself dirty; the host
  takes one serialized write covering any number of mutations and writes it where
  and when it chooses. `with_file` is unchanged.

### Fixed
- **A loaded bus snapshot is held to the ring and size caps.** The caps applied
  only to a live publish, so a hand-edited snapshot could load any number of
  events of any size. Past `RING_CAP` only the newest events are kept, and an
  event over the field or message cap is dropped.

## [0.1.0] - 2026-09-01

### Added
- **Initial extraction from atrium.** The coordination layer, the shared `board`
  and the pub/sub `bus`, lifted out of atrium into its own one-concern org crate,
  unchanged in behavior and API.
  - `board`: a schemaless `key → fields` source-of-truth tracker: `set` (merge;
    empty value clears a field), `get`, `list`, `del`, `updated_by`/`updated_ms`
    accountability, and opt-in atomic snapshot persistence (`ENV_BOARD`).
  - `bus`: topic-routed pub/sub: `publish` (`fyi` vs `decision_needed`),
    `subscribe`/`unsubscribe` (with a `*` firehose), `feed` (pull by topic with a
    resumable cursor, no self-echo), `resolve`, and `pending_decisions`.
    Backpressure built in: no echo, pull-not-push, a per-agent publish rate cap,
    and a bounded event ring. Opt-in atomic snapshot persistence (`ENV_BUS`).
  - Pure logic over in-memory state; zero third-party dependencies (std + the org
    `json` crate). 16 unit tests carried over from atrium.
