# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-09-01

### Added
- **Initial extraction from amux.** The coordination layer, the shared `board`
  and the pub/sub `bus`, lifted out of amux into its own one-concern org crate,
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
    `json` crate). 16 unit tests carried over from amux.
