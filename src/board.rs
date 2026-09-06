//! The shared **board**: durable source-of-truth state for a coordinating team.
//!
//! A schemaless `key → fields` tracker owned by the host's broker process.
//! Because a single broker owns the instance and every participant talks to it
//! over one channel, the board is a plain map behind that channel: **single
//! process, single writer, so no locking and no consensus.** This is the "what is
//! currently true" half of the coordination layer (`auth: DONE, owner: Max, url:
//! …`), the durable complement to an ephemeral message stream: a lead reads
//! current state on demand instead of re-scraping transcripts.
//!
//! Optionally mirrored to a snapshot file (the host names it via [`ENV_BOARD`])
//! so a board survives a restart. Pure logic + a best-effort atomic file write;
//! unit-tested without a terminal.
//!
//! Paired with the [`crate::bus`] (the active event stream) as the two halves of
//! the coordination layer.

use json::{Number, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Environment knob naming a snapshot file to persist the board to. Unset ⇒ the
/// board is in-memory only (lost on exit). Opt-in, on top of `--allow-ctl`.
pub const ENV_BOARD: &str = "AMUX_BOARD";

/// Default lease length for a [`Board::claim`] (and the renewal an owner's
/// [`Board::set`] grants): 5 minutes. Deliberately generous: a lease shorter
/// than a real unit of work would expire mid-task and let a second agent
/// double-claim, recreating the very collision claiming exists to prevent. An
/// actively-working owner renews on every `set`, so only a stalled/dead owner
/// ever lapses.
pub const DEFAULT_LEASE_MS: u64 = 5 * 60 * 1000;

/// One board entry: freeform field→value pairs plus who/when last touched it,
/// plus an optional **claim** (a soft lease) so a coordinating team can divide
/// work without collisions. The claim is typed (not a freeform field) so a stray
/// `set` can't stomp it.
/// `BTreeMap` so fields render in a stable (sorted) order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entry {
    pub fields: BTreeMap<String, String>,
    /// The role (or pane) that last wrote this entry: accountability, not auth.
    pub updated_by: Option<String>,
    pub updated_ms: u64,
    /// The role (or pane) that currently **holds** this task, if any: set by
    /// [`Board::claim`], renewed by that owner's [`Board::set`], cleared by
    /// [`Board::release`]. Accountability + collision-avoidance, not auth.
    pub claimed_by: Option<String>,
    /// Absolute epoch-ms at which the claim lapses (`0` ⇒ no active lease). Once
    /// `now >= lease_ms` the task is claimable again even if `claimed_by` is still
    /// set, so a crashed owner never strands its task.
    pub lease_ms: u64,
}

impl Entry {
    /// Is this entry actively held *by someone other than* `who` at `now_ms`? A
    /// lapsed lease (`now >= lease_ms`) or a claim held by `who` itself is not a
    /// block; both let `who` (re)claim.
    fn held_against(&self, who: &str, now_ms: u64) -> bool {
        match &self.claimed_by {
            Some(owner) => owner != who && now_ms < self.lease_ms,
            None => false,
        }
    }
}

/// The result of a [`Board::claim`]: the caller either now holds the task or was
/// told who does. On `Denied` the board is left untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// The caller holds the task; the entry carries the fresh `claimed_by`/lease.
    Granted(Entry),
    /// Someone else holds it: `holder` until `lease_ms` (epoch-ms). No write made.
    Denied { holder: String, lease_ms: u64 },
}

/// The shared board: keyed entries with an optional snapshot file.
#[derive(Debug, Default)]
pub struct Board {
    entries: BTreeMap<String, Entry>,
    path: Option<PathBuf>,
}

impl Board {
    /// An in-memory board (no persistence).
    pub fn new() -> Self {
        Self::default()
    }

    /// A board backed by a snapshot file, loading any existing state. A missing,
    /// empty, or corrupt file simply starts empty; persistence is best-effort and
    /// never fatal.
    pub fn with_file(path: PathBuf) -> Self {
        let entries = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| parse_snapshot(&s))
            .unwrap_or_default();
        Board {
            entries,
            path: Some(path),
        }
    }

    /// Merge `fields` into `key`'s entry (creating it if absent), stamping who and
    /// when. A field with an **empty** value is cleared; unmentioned fields are
    /// kept. Returns the updated entry. Persists if backed by a file.
    pub fn set(
        &mut self,
        key: &str,
        fields: &[(String, String)],
        by: Option<&str>,
        now_ms: u64,
    ) -> Entry {
        let e = self.entries.entry(key.to_string()).or_default();
        for (f, v) in fields {
            if v.is_empty() {
                e.fields.remove(f);
            } else {
                e.fields.insert(f.clone(), v.clone());
            }
        }
        // Any update *by the current claim holder* renews the lease: an
        // actively-working owner's status writes are its heartbeat, so it never
        // loses its claim, while a stalled owner's lease still lapses. A set by
        // anyone else touches fields only and leaves the claim alone (the board
        // is cooperative: a lead can annotate a teammate's task without seizing
        // it).
        if let (Some(setter), Some(owner)) = (by, e.claimed_by.as_deref()) {
            if setter == owner {
                e.lease_ms = now_ms + DEFAULT_LEASE_MS;
            }
        }
        e.updated_by = by.map(str::to_string);
        e.updated_ms = now_ms;
        let out = e.clone();
        self.persist();
        out
    }

    /// Atomically **claim** `key` for `owner` with a `ttl_ms` lease. Because the
    /// broker is single-writer this check-then-write has no race: the first
    /// caller wins and every later caller is told the holder: the primitive that
    /// turns "everyone grabs the same task, then everyone flees it" into "first
    /// grabs, the rest fan out to what's left."
    ///
    /// Grants when the task is free, its lease has lapsed, or `owner` already
    /// holds it (idempotent renew). Denies, writing nothing, when someone else
    /// holds an unexpired lease.
    pub fn claim(&mut self, key: &str, owner: &str, ttl_ms: u64, now_ms: u64) -> Claim {
        let e = self.entries.entry(key.to_string()).or_default();
        if e.held_against(owner, now_ms) {
            return Claim::Denied {
                holder: e.claimed_by.clone().unwrap_or_default(),
                lease_ms: e.lease_ms,
            };
        }
        e.claimed_by = Some(owner.to_string());
        e.lease_ms = now_ms + ttl_ms;
        e.updated_by = Some(owner.to_string());
        e.updated_ms = now_ms;
        let out = e.clone();
        self.persist();
        Claim::Granted(out)
    }

    /// **Release** `key`'s claim, clearing the holder and lease but keeping its
    /// fields (the task's recorded state outlives who was working it). `true` if
    /// the entry existed. Idempotent; persists if backed by a file.
    pub fn release(&mut self, key: &str, now_ms: u64) -> bool {
        match self.entries.get_mut(key) {
            Some(e) => {
                e.claimed_by = None;
                e.lease_ms = 0;
                e.updated_ms = now_ms;
                self.persist();
                true
            }
            None => false,
        }
    }

    /// One entry, if present.
    pub fn get(&self, key: &str) -> Option<&Entry> {
        self.entries.get(key)
    }

    /// Every `(key, entry)`, key-sorted.
    pub fn list(&self) -> Vec<(&String, &Entry)> {
        self.entries.iter().collect()
    }

    /// Remove an entry; `true` if it existed. Persists if backed by a file.
    pub fn del(&mut self, key: &str) -> bool {
        let had = self.entries.remove(key).is_some();
        if had {
            self.persist();
        }
        had
    }

    fn persist(&self) {
        if let Some(p) = &self.path {
            let _ = write_atomic(p, &snapshot(&self.entries));
        }
    }
}

/// Render an entry as a JSON value:
/// `{"by":…, "ms":…, "claimed_by":…|null, "lease_ms":…, "fields":{…}}`.
pub fn entry_to_value(e: &Entry) -> Value {
    let fields: Vec<(String, Value)> = e
        .fields
        .iter()
        .map(|(f, v)| (f.clone(), Value::String(v.clone())))
        .collect();
    Value::Object(vec![
        (
            "by".to_string(),
            e.updated_by
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        ("ms".to_string(), Value::Number(Number::Int(e.updated_ms as i64))),
        (
            "claimed_by".to_string(),
            e.claimed_by.clone().map(Value::String).unwrap_or(Value::Null),
        ),
        (
            "lease_ms".to_string(),
            Value::Number(Number::Int(e.lease_ms as i64)),
        ),
        ("fields".to_string(), Value::Object(fields)),
    ])
}

/// Render a `(key, entry)` roll-up as a JSON array of `{"key":…, "by":…, "ms":…,
/// "fields":{…}}` objects.
pub fn entries_to_value(entries: &[(&String, &Entry)]) -> Value {
    let arr = entries
        .iter()
        .map(|(k, e)| {
            let mut members = vec![("key".to_string(), Value::String((*k).clone()))];
            if let Value::Object(o) = entry_to_value(e) {
                members.extend(o);
            }
            Value::Object(members)
        })
        .collect();
    Value::Array(arr)
}

/// Serialize the whole board to a snapshot: `{key:{by,ms,fields}}`.
fn snapshot(entries: &BTreeMap<String, Entry>) -> String {
    let obj = entries
        .iter()
        .map(|(k, e)| (k.clone(), entry_to_value(e)))
        .collect();
    Value::Object(obj).to_string()
}

/// Parse a snapshot back into entries. `None` if the text is not a JSON object.
fn parse_snapshot(s: &str) -> Option<BTreeMap<String, Entry>> {
    let v = json::parse(s).ok()?;
    let obj = v.as_object()?;
    let mut out = BTreeMap::new();
    for (k, ev) in obj {
        let updated_by = ev.get("by").and_then(Value::as_str).map(str::to_string);
        let updated_ms = ev.get("ms").and_then(Value::as_i64).unwrap_or(0).max(0) as u64;
        // Claim fields are absent in pre-lease snapshots; default to unclaimed.
        let claimed_by = ev.get("claimed_by").and_then(Value::as_str).map(str::to_string);
        let lease_ms = ev.get("lease_ms").and_then(Value::as_i64).unwrap_or(0).max(0) as u64;
        let mut fields = BTreeMap::new();
        if let Some(fo) = ev.get("fields").and_then(Value::as_object) {
            for (f, fv) in fo {
                if let Some(val) = fv.as_str() {
                    fields.insert(f.clone(), val.to_string());
                }
            }
        }
        out.insert(
            k.clone(),
            Entry {
                fields,
                updated_by,
                updated_ms,
                claimed_by,
                lease_ms,
            },
        );
    }
    Some(out)
}

/// Write `contents` to `path` atomically (temp file + rename), so a reader never
/// sees a half-written board.
fn write_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn set_creates_then_merges_fields() {
        let mut b = Board::new();
        b.set("auth", &f(&[("status", "WIP"), ("owner", "Max")]), Some("cto"), 100);
        // A second set merges: status updated, owner kept, url added.
        let e = b.set("auth", &f(&[("status", "DONE"), ("url", "http://x")]), Some("Max"), 200);
        assert_eq!(e.fields.get("status").map(String::as_str), Some("DONE"));
        assert_eq!(e.fields.get("owner").map(String::as_str), Some("Max"));
        assert_eq!(e.fields.get("url").map(String::as_str), Some("http://x"));
        assert_eq!(e.updated_by.as_deref(), Some("Max"));
        assert_eq!(e.updated_ms, 200);
    }

    #[test]
    fn empty_value_clears_a_field() {
        let mut b = Board::new();
        b.set("t", &f(&[("blocker", "waiting on api")]), None, 1);
        let e = b.set("t", &f(&[("blocker", "")]), None, 2);
        assert!(!e.fields.contains_key("blocker"), "empty value clears the field");
    }

    #[test]
    fn get_list_and_del() {
        let mut b = Board::new();
        b.set("a", &f(&[("status", "DONE")]), None, 1);
        b.set("b", &f(&[("status", "WIP")]), None, 1);
        assert!(b.get("a").is_some());
        assert_eq!(b.list().len(), 2);
        // list is key-sorted.
        assert_eq!(b.list()[0].0, "a");
        assert!(b.del("a"));
        assert!(!b.del("a"), "second delete is a no-op");
        assert!(b.get("a").is_none());
        assert_eq!(b.list().len(), 1);
    }

    #[test]
    fn claim_grants_when_free_and_denies_a_second_taker() {
        let mut b = Board::new();
        // First taker wins.
        match b.claim("t1", "scout", 1000, 100) {
            Claim::Granted(e) => {
                assert_eq!(e.claimed_by.as_deref(), Some("scout"));
                assert_eq!(e.lease_ms, 1100);
            }
            other => panic!("first claim should grant, got {other:?}"),
        }
        // A different agent, while the lease is live, is denied and told who holds it.
        match b.claim("t1", "maker", 1000, 200) {
            Claim::Denied { holder, lease_ms } => {
                assert_eq!(holder, "scout");
                assert_eq!(lease_ms, 1100);
            }
            other => panic!("second claim should be denied, got {other:?}"),
        }
        // The board was not mutated by the denied claim.
        assert_eq!(b.get("t1").unwrap().claimed_by.as_deref(), Some("scout"));
    }

    #[test]
    fn claim_is_reclaimable_after_the_lease_lapses() {
        let mut b = Board::new();
        b.claim("t", "scout", 1000, 100); // lease until 1100
        // At now=1100 the lease has lapsed (>=), so a new agent may take it.
        match b.claim("t", "maker", 1000, 1100) {
            Claim::Granted(e) => assert_eq!(e.claimed_by.as_deref(), Some("maker")),
            other => panic!("expired lease should be reclaimable, got {other:?}"),
        }
    }

    #[test]
    fn same_owner_reclaim_is_an_idempotent_renew() {
        let mut b = Board::new();
        b.claim("t", "scout", 1000, 100);
        match b.claim("t", "scout", 1000, 500) {
            Claim::Granted(e) => assert_eq!(e.lease_ms, 1500, "renew extends the lease"),
            other => panic!("same-owner reclaim should renew, got {other:?}"),
        }
    }

    #[test]
    fn owners_set_renews_the_lease_but_a_strangers_does_not() {
        let mut b = Board::new();
        b.claim("t", "scout", 1000, 100); // lease until 1100
        // The owner updating status renews to now + DEFAULT_LEASE_MS.
        let e = b.set("t", &f(&[("status", "WIP")]), Some("scout"), 200);
        assert_eq!(e.lease_ms, 200 + DEFAULT_LEASE_MS);
        // A stranger annotating the task updates fields but leaves the claim intact.
        let e = b.set("t", &f(&[("note", "looks good")]), Some("lead"), 300);
        assert_eq!(e.claimed_by.as_deref(), Some("scout"));
        assert_eq!(e.lease_ms, 200 + DEFAULT_LEASE_MS, "stranger's set must not renew");
        assert_eq!(e.fields.get("note").map(String::as_str), Some("looks good"));
    }

    #[test]
    fn release_clears_the_claim_but_keeps_fields() {
        let mut b = Board::new();
        b.set("t", &f(&[("status", "WIP")]), Some("scout"), 1);
        b.claim("t", "scout", 1000, 100);
        assert!(b.release("t", 200));
        let e = b.get("t").unwrap();
        assert!(e.claimed_by.is_none(), "release clears the holder");
        assert_eq!(e.lease_ms, 0, "release clears the lease");
        assert_eq!(e.fields.get("status").map(String::as_str), Some("WIP"), "fields survive");
        // Now anyone can claim it.
        assert!(matches!(b.claim("t", "maker", 1000, 300), Claim::Granted(_)));
        assert!(!b.release("missing", 1), "release of an absent key is false");
    }

    #[test]
    fn snapshot_round_trips_the_claim() {
        let mut b = Board::new();
        b.claim("t", "scout", 1000, 100);
        let snap = snapshot(
            &b.list().into_iter().map(|(k, e)| (k.clone(), e.clone())).collect(),
        );
        let parsed = parse_snapshot(&snap).unwrap();
        let e = &parsed["t"];
        assert_eq!(e.claimed_by.as_deref(), Some("scout"));
        assert_eq!(e.lease_ms, 1100);
    }

    #[test]
    fn pre_lease_snapshot_parses_as_unclaimed() {
        // An old snapshot with no claim keys must load, defaulting to unclaimed.
        let old = r#"{"auth":{"by":"cto","ms":42,"fields":{"status":"DONE"}}}"#;
        let parsed = parse_snapshot(old).unwrap();
        let e = &parsed["auth"];
        assert!(e.claimed_by.is_none());
        assert_eq!(e.lease_ms, 0);
        assert_eq!(e.fields.get("status").map(String::as_str), Some("DONE"));
    }

    #[test]
    fn snapshot_round_trips() {
        let mut b = Board::new();
        b.set("auth", &f(&[("status", "DONE"), ("owner", "Max")]), Some("cto"), 42);
        b.set("bill", &f(&[("status", "BLOCKED")]), Some("Vic"), 7);
        let snap = snapshot(
            &b.list()
                .into_iter()
                .map(|(k, e)| (k.clone(), e.clone()))
                .collect(),
        );
        let parsed = parse_snapshot(&snap).unwrap();
        assert_eq!(parsed.len(), 2);
        let auth = &parsed["auth"];
        assert_eq!(auth.fields.get("status").map(String::as_str), Some("DONE"));
        assert_eq!(auth.fields.get("owner").map(String::as_str), Some("Max"));
        assert_eq!(auth.updated_by.as_deref(), Some("cto"));
        assert_eq!(auth.updated_ms, 42);
    }

    #[test]
    fn entries_to_value_carries_key_and_fields() {
        let mut b = Board::new();
        b.set("auth", &f(&[("status", "DONE")]), Some("cto"), 1);
        let v = entries_to_value(&b.list());
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].get("key").and_then(Value::as_str), Some("auth"));
        assert_eq!(
            arr[0]
                .get("fields")
                .and_then(|f| f.get("status"))
                .and_then(Value::as_str),
            Some("DONE")
        );
    }
}
