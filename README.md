# abus

**The coordination layer for a team of agents**: a shared **board** and a
topic-routed pub/sub **bus**, as pure, snapshot-persistable logic. Part of
[nativelite](https://github.com/nativelite): standard library plus the org `json`
crate, **zero third-party dependencies**.

A single host process (a broker) owns each instance and every participant talks
to it over one channel, so the state is a plain map / ring behind that channel:
**single writer, no locking, no consensus.** abus is transport-agnostic: it holds
the state and the rules; the host wires it to its own IPC and renders it. It was
extracted from [amux](https://github.com/nativelite/amux), the multi-agent
terminal that first grew this layer.

## The two halves

| | Board | Bus |
| --- | --- | --- |
| Answers | *"what is currently true?"* | *"what just happened, and does anyone need to act?"* |
| Shape | durable `key → fields` state | append-only stream of structured events |
| Read | on demand (`get` / `list`) | pull by topic (`feed`), newest resumable via a cursor |

### Board

A schemaless `key → fields` tracker. `set` merges fields into an entry (an empty
value clears a field); every write records who made it and when; `AMUX_BOARD`-style
snapshot persistence is opt-in via [`board::ENV_BOARD`].

```rust
use abus::board::Board;
let mut b = Board::new();
b.set("auth", &[("status".into(), "DONE".into()), ("owner".into(), "Max".into())], Some("dev_1"), 1_700_000_000_000);
let e = b.get("auth").unwrap();
assert_eq!(e.fields["status"], "DONE");
```

### Bus

Topic-routed pub/sub with **structured events, not prose**, in two urgency
classes: [`bus::Kind::Fyi`] (cheap, informational, lands on the feed) and
[`bus::Kind::DecisionNeeded`] (an escalation that needs a human/lead answer).
**Backpressure is built in:** no echo of your own events, pull-not-push (you only
pull the topics you subscribed to), a per-agent publish rate cap, and a bounded
ring so the log never grows without bound.

```rust
use abus::bus::{Bus, Kind};
let mut bus = Bus::new();
bus.subscribe("lead", &["deploy".into()]);
bus.publish("deploy", Kind::Fyi, Some("dev_1"), &[("msg".into(), "merged".into())], 1_700_000_000_000).unwrap();
let feed = bus.feed("lead", 0);       // lead pulls the deploy topic; never its own events
assert_eq!(feed.len(), 1);
```

Snapshot the feed + subscriptions across a restart with [`bus::ENV_BUS`].

## Design rules

- **Zero third-party runtime dependencies.** std + the org `json` crate only.
- **One concern.** Coordination state and its rules: no I/O loop, no sockets, no
  rendering. The host owns those.
- **Pure and testable.** Every method is a function over in-memory state plus a
  best-effort atomic snapshot write; the whole crate is unit-tested without a
  terminal.

## Develop

```
python dev.py check   # zero-dependency guard + cargo test (the gate)
python dev.py test    # cargo test (unit + doctests)
python dev.py guard   # dependency guard only
```

## License

MIT: see [LICENSE](LICENSE). nativelite ships its packages to give the
technology away.
