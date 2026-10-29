# 25 — long-term-memory

Long-term memory (`design/45-Long-Term-Memory.md`): `Memory` in
`datalium-strata-ai`, over the engine's `MemoryStore` port. `remember` and
`remember_many` embed a text and store it with its vector. `recall` embeds a
text and returns the most similar records, by exact cosine similarity. The
store never calls a model. It takes vectors, and `Memory` makes them with an
`Embedder`.

The demo is an incident desk. Past incidents are remembered for two tenants.
A graph, `Recall -> Diagnose -> Remember`, recalls the incidents most like a
new one, reuses the closest one's fix, and remembers the new incident, so the
next run can recall it. The tenant comes from the run's metadata (`tenant`,
set with `RunConfig::with_metadata`), not from the nodes' code.

**The embedder is a stand-in.** `BagOfWords` is a small deterministic
`Embedder`. Each word (lower-cased, stop words dropped, a crude suffix strip)
adds 1 to one of 256 buckets, chosen by an FNV-1a hash. FNV-1a is fixed by its
definition, where std's `DefaultHasher` may change between Rust releases.
Texts that share words really are closer, so the ranking is meaningful,
though far cruder than a real model's. The store is the engine's
`InMemoryMemoryStore` (`datalium_strata::memory`) with a clock the demo moves
by hand (`with_clock`). No network, no API key, and every run prints the same
thing.

The Postgres (pgvector), SQLite (sqlite-vec) and Redis 8 (Query Engine)
stores in `datalium-strata-memory` (`PostgresMemoryStore`,
`SqliteMemoryStore`, `RedisMemoryStore`) implement the same `MemoryStore` /
`MemoryRetention` traits, and the code above the store would not change. This
demo doesn't use them, so it needs no server and no native extension.

The .NET twin is
[`strata-demos-dotnet/demos/25-long-term-memory`](https://github.com/datalium/strata-demos-dotnet/tree/main/demos/25-long-term-memory).
It has the same six steps and the same checks, and `BagOfWords` is the same
algorithm, so both make the same vectors and print the same similarities.
Where the engines differ:

- **The clock.** The .NET store takes a `FakeTimeProvider`. Rust's
  `InMemoryMemoryStore::with_clock` takes a `MemoryClock`, a closure returning
  a `DateTime<Utc>`; the demo's reads a time behind a `Mutex` that it moves by
  whole days
- **The tenant outside a run.** `Memory`'s calls take
  `impl Into<Option<&NodeContext>>`: `&ctx` inside a node (the tenant comes
  from the run's metadata), `None` outside one (the tenant is the entry's or
  the options' own, else `""`). .NET makes the context an optional parameter
- **The label.** `Memory::label()` returns `Result<String, MemoryError>`
  (`demo/bow-256`), where .NET has a `Model` property. A bad input or a failed
  embedding call is a `MemoryError` (`Store` or `Embedding`), which `?` in a
  fallible node turns into a `NodeError`
- **Content** is a `serde_json::Map`; the demo's keys are `root_cause` and
  `action` where .NET's are `rootCause` and `action`. Nothing crosses between
  the two

## What it demonstrates

- **Remembering** (step 1). `remember_many` checks every entry, embeds all six
  texts in **one** embedding call, and writes them in one `upsert_many`. Each
  record keeps its text (`text`), the application's JSON (`content`: root
  cause and action) and string metadata (`service`). The vectors are labelled
  `demo/bow-256`, which is `<provider>/<model>` from the embedder's
  `provider()` and `default_model()`
- **Recall inside a graph** (step 2). `Recall` asks for at most 3 matches
  with `min_similarity(0.5)`, excluding the incident's own id. `Diagnose`
  reuses the closest match's fix, or escalates when nothing is close enough.
  `Remember` stores the new incident. Both memory nodes are
  `add_async_fallible_node`s that pass their `NodeContext`. Each run makes two
  embedding calls, observed as `ModelCallStarted` events with operation
  `embeddings`. The second run recalls `inc-201`, which the first run
  remembered
- **Tenants** (step 2). Both tenants have an incident `inc-101` with the same
  symptoms and different causes. The key is (namespace, tenant, id), so both
  exist. A globex run recalls only globex's record and reuses globex's fix. A
  recall with no tenant reads only tenant-less records, here none: it never
  means "every tenant"
- **Filters** (step 3), applied before ranking: metadata equality
  (`with_metadata("service", "search")`), `min_similarity`, `limit` (the top N
  of the same ranking) and `exclude`. `inc-101` and `inc-103` share exactly
  the same query words, so they tie at 0.655, and a tie is ordered by id
- **Namespaces** (step 3). The runbook `rb-oom` is in namespace `runbooks`. A
  recall in `incidents` never returns it, however similar it is
- **Vectors are labelled by model** (step 4). A second `Memory` whose embedder
  names `bow-256-v2` makes the very same vectors, but its recall finds
  nothing. A search reads only its own label's records, so two models'
  vectors are never compared
- **An upsert keeps `created_at`** (step 5). Remembering `inc-101` again 300
  days later replaces its content and moves `updated_at`. `created_at` stays
  at the first write. The timestamps are the store's, from its clock, read
  back with `MemoryStore::get`
- **Purging by age** (step 6).
  `MemoryRetention::purge(&MemoryPurge::new(cutoff).namespace("incidents"))`
  deletes the records last written before the cutoff, for every tenant. That
  removes 10 records. `inc-101` survives because its upsert at day 300 is
  recent, and the runbook survives because it is in another namespace.
  `MemoryStore::list` shows what is left

Each claim is checked when the demo runs, and the demo exits non-zero if one
fails.

## What it does *not* cover

- **The SQL and Redis stores**, their dimensions check on open, the HNSW
  index option and their metrics (see above).
  [`12-pluggable-storage`](../12-pluggable-storage) shows the same
  swap-the-store idea for checkpoints and history.
- **Hosting**: `datalium-strata-host`'s `Host::retain_memory` with a
  `MemoryRetentionDriver`, which calls `purge` on a timer, and
  `StrataSettings`' memory store options. The demo builds `Memory` by hand and
  calls `purge` itself. [`28-hosting`](../28-hosting) covers the host.
- **Re-embedding after a model change**: list a namespace with `list` and
  remember each record's `text` again with the new model. The key doesn't
  include the model, so each new record replaces its old one.
- **Embedding failures.** A failed embedding call is a model call's failure,
  `MemoryError::Embedding` with a `NodeError` that is transient with the
  provider's hint or fatal, which a node's `RecoveryPolicy` can retry. Nothing
  is stored when it fails. The stand-in never fails.
- **Bad input**, refused with `MemoryError::Store` before any I/O. An empty id
  or a limit out of range is refused before anything is embedded. A vector of
  the wrong length, or one that is all zeros, is refused by the store.
- **`Memory::with_model` and `with_tenant_key`**, which set the label and the
  metadata key the tenant is read from.

## Run it

```bash
cargo run -p long-term-memory
```

Output of a real run (identical on every run):

```
=== Step 1: remember past incidents (one embedding call for six entries) ===
  acme: 6 incidents remembered in 1 embedding call(s), labelled 'demo/bow-256'
  globex: 1 incident (same id inc-101, same symptoms, another cause); acme: 1 runbook in namespace 'runbooks'

=== Step 2: an incident desk that recalls similar incidents (Recall -> Diagnose -> Remember) ===
  [acme] inc-201: "Checkout pods crash looping again, out of memory after this morning's deploy"
      recalled inc-101 (0.802): a memory leak in the v2.3 image
      -> likely a memory leak in the v2.3 image (like inc-101); action: roll back the deploy
  [acme] inc-202: "Checkout out of memory, pods crash looping after the deploy"
      recalled inc-101 (0.926): a memory leak in the v2.3 image
      recalled inc-201 (0.866): likely a memory leak in the v2.3 image (like inc-101)
      -> likely a memory leak in the v2.3 image (like inc-101); action: roll back the deploy
  [globex] gx-7: "Checkout pods crash looping again, out of memory after this morning's deploy"
      recalled inc-101 (0.802): a noisy neighbour on the node
      -> likely a noisy neighbour on the node (like inc-101); action: move the pods to another node pool
  [acme] inc-203: "Billing emails delayed, the mail queue backlog keeps growing"
      -> no similar incident; action: page the on-call engineer
  The same recall with no tenant (no run metadata): 0 match(es)

=== Step 3: filters -- metadata, minimum similarity, limit, excluded ids, namespace ===
  everything (limit 10)      inc-101 0.655, inc-103 0.655, inc-202 0.471, inc-201 0.408, inc-102 0.000, inc-104 0.000, inc-105 0.000, inc-106 0.000, inc-203 0.000
  metadata service=search    inc-103 0.655
  min_similarity 0.5         inc-101 0.655, inc-103 0.655
  limit 3                    inc-101 0.655, inc-103 0.655, inc-202 0.471
  exclude [inc-101]          inc-103 0.655, inc-202 0.471, inc-201 0.408, inc-102 0.000, inc-104 0.000, inc-105 0.000, inc-106 0.000, inc-203 0.000
  namespace 'runbooks': rb-oom 0.696

=== Step 4: another model's memory reads none of these records ===
  'demo/bow-256-v2' recall: 0 match(es); 'demo/bow-256' recall: 9

=== Step 5: remembering inc-101 again 300 days later ===
  created 2026-01-05, updated 2026-11-01, root cause: a memory leak in the v2.3 image (postmortem PM-17)

=== Step 6: 400 days in, purge incidents not written for 365 days ===
  Purged 10 record(s) last written before 2026-02-09.
  Left: acme incidents [inc-101], globex incidents [], acme runbooks [rb-oom]

=== Done: memories were recalled by meaning, kept apart by tenant, namespace and model, updated in place and purged by age. ===
```

The similarities are the bag-of-words embedder's, the same figures as the
.NET twin's. A real model gives other numbers, and usually much higher ones:
StrataOps measured 0.98 to 1.00 between incidents on OpenAI's embeddings,
which is why the design says to rank rather than rely on a threshold. A
`0.000` match shares no word with the query. With no `min_similarity` it is
still returned, last. The dates come from the demo's clock, which starts at
2026-01-05.

## Files

- `src/main.rs` — the six steps, the incident desk graph, and `BagOfWords`
  (the stand-in embedder)
- `Cargo.toml` — depends on `datalium-strata`, `datalium-strata-ai` and
  `datalium-strata-memory` (for `InMemoryCheckpointStore`), plus `chrono`,
  `serde`, `serde_json` and `tokio`
