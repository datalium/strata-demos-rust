//! Long-term memory: remember by meaning, recall by similarity
//! (`design/45-Long-Term-Memory.md`).
//!
//! `Memory` (in `datalium-strata-ai`) joins a memory store (`MemoryStore`)
//! and an embedder (`Embedder`). `remember` embeds a text and stores it with
//! the vector; `recall` embeds a text and returns the most similar records by
//! cosine similarity. The store never calls a model: it takes vectors. Here
//! the store is the engine's `InMemoryMemoryStore`, and the embedder is
//! `BagOfWords`, a deterministic stand-in written below: each word is hashed
//! into one of 256 buckets, so texts that share words really are closer.
//!
//! The demo is an incident desk. Past incidents are remembered for two
//! tenants. A graph -- Recall -> Diagnose -> Remember -- recalls the incidents
//! most like a new one, reuses the closest one's fix, and remembers the new
//! incident, so the next run can recall it. The tenant comes from the run's
//! metadata (`tenant`), never from the nodes' code. Then the demo shows the
//! search's filters, that vectors are labelled by model, that an upsert keeps
//! a record's creation time, and purging by age.
//!
//! No live model and no API key. The store's clock is a closure over a time
//! the demo moves by hand (`with_clock`), so a year passes in a line.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use datalium_strata::BoxFuture;
use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::context::{NodeContext, RunConfig};
use datalium_strata::graph::{CompiledGraph, StateGraph};
use datalium_strata::memory::{
    InMemoryMemoryStore, MemoryKey, MemoryMatch, MemoryPurge, MemoryQuery, MemoryRetention,
    MemoryStore,
};
use datalium_strata::observability::{RunEventChannel, RunEventKind};
use datalium_strata::store::CheckpointStore;
use datalium_strata_ai::{
    ChatError, Embedder, EmbeddingRequest, EmbeddingResponse, Memory, MemoryEntry, ModelUsage,
    RecallOptions,
};
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// How long the demo waits for a run's events before calling it a failure.
const PATIENCE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Incident {
    id: String,
    service: String,
    symptoms: String,
    similar: Vec<Recalled>,
    diagnosis: String,
    action: String,
}

impl Incident {
    fn new(id: &str, service: &str, symptoms: &str) -> Self {
        Self {
            id: id.to_string(),
            service: service.to_string(),
            symptoms: symptoms.to_string(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Recalled {
    id: String,
    similarity: f64,
    root_cause: String,
    action: String,
}

impl From<&MemoryMatch> for Recalled {
    fn from(found: &MemoryMatch) -> Self {
        let field = |name: &str| {
            found
                .record
                .content
                .as_ref()
                .and_then(|content| content.get(name))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        Self {
            id: found.record.id.clone(),
            similarity: found.similarity,
            root_cause: field("root_cause"),
            action: field("action"),
        }
    }
}

#[tokio::main]
async fn main() {
    let start = Utc.with_ymd_and_hms(2026, 1, 5, 9, 0, 0).unwrap();
    let now = Arc::new(Mutex::new(start));
    let store = Arc::new(
        InMemoryMemoryStore::new(BagOfWords::DIMENSIONS).with_clock({
            let now = now.clone();
            Arc::new(move || *now.lock().unwrap())
        }),
    );
    let advance = |days: i64| *now.lock().unwrap() += TimeDelta::days(days);
    let embedder = Arc::new(BagOfWords::new("demo", "bow-256"));
    let memory = Memory::new(store.clone(), embedder.clone());
    let label = memory.label().expect("the embedder names its model");

    // --- Step 1: remember past incidents, for two tenants. ---

    println!("=== Step 1: remember past incidents (one embedding call for six entries) ===");
    memory
        .remember_many(
            vec![
                past(
                    "acme",
                    "inc-101",
                    "checkout",
                    "Checkout pods crash looping, out of memory killed after deploy",
                    "a memory leak in the v2.3 image",
                    "roll back the deploy",
                ),
                past(
                    "acme",
                    "inc-102",
                    "checkout",
                    "Checkout latency spike, database connection pool exhausted",
                    "a connection pool too small for the sale",
                    "raise the pool size",
                ),
                past(
                    "acme",
                    "inc-103",
                    "search",
                    "Search pods out of memory killed during the nightly reindex",
                    "a reindex batch too large",
                    "halve the reindex batch",
                ),
                past(
                    "acme",
                    "inc-104",
                    "payments",
                    "Payments API returns 502 bad gateway, upstream certificate expired",
                    "an expired upstream certificate",
                    "renew the certificate",
                ),
                past(
                    "acme",
                    "inc-105",
                    "checkout",
                    "Checkout error rate high after a new feature flag was enabled",
                    "a bug behind the flag",
                    "turn the flag off",
                ),
                past(
                    "acme",
                    "inc-106",
                    "auth",
                    "Login failures, TLS handshake errors after certificate rotation",
                    "a missing intermediate certificate",
                    "redeploy the full chain",
                ),
            ],
            None,
        )
        .await
        .expect("remember_many");
    println!(
        "  acme: 6 incidents remembered in {} embedding call(s), labelled '{label}'",
        embedder.calls()
    );
    check(
        embedder.calls() == 1,
        "expected remember_many to embed every entry in one call",
    );
    check(
        label == "demo/bow-256",
        "expected the label <provider>/<model> from the embedder",
    );

    // Another tenant has an incident with the same id and the very same
    // symptoms, and another cause.
    memory
        .remember(
            past(
                "globex",
                "inc-101",
                "checkout",
                "Checkout pods crash looping, out of memory killed after deploy",
                "a noisy neighbour on the node",
                "move the pods to another node pool",
            ),
            None,
        )
        .await
        .expect("remember");
    // Another namespace: a runbook, not an incident.
    memory
        .remember(
            MemoryEntry::new(
                "runbooks",
                "rb-oom",
                "Pods out of memory killed: raise the memory limit or roll back the deploy",
            )
            .with_tenant("acme"),
            None,
        )
        .await
        .expect("remember");
    println!(
        "  globex: 1 incident (same id inc-101, same symptoms, another cause); acme: 1 runbook in namespace 'runbooks'"
    );

    // --- Step 2: the incident desk graph. ---

    println!();
    println!(
        "=== Step 2: an incident desk that recalls similar incidents (Recall -> Diagnose -> Remember) ==="
    );
    let desk = incident_desk(memory.clone());

    let first = handle(
        &desk,
        "acme",
        Incident::new(
            "inc-201",
            "checkout",
            "Checkout pods crash looping again, out of memory after this morning's deploy",
        ),
    )
    .await;
    check(
        first.similar.first().is_some_and(|m| m.id == "inc-101"),
        "expected acme's inc-101 to be the closest",
    );
    check(
        first.action == "roll back the deploy",
        "expected inc-101's fix to be reused",
    );
    check(
        first.similar.iter().all(|m| m.id.starts_with("inc-")),
        "expected only incidents, never the runbook",
    );

    let second = handle(
        &desk,
        "acme",
        Incident::new(
            "inc-202",
            "checkout",
            "Checkout out of memory, pods crash looping after the deploy",
        ),
    )
    .await;
    check(
        second.similar.iter().any(|m| m.id == "inc-201"),
        "expected the incident remembered by the last run to be recalled",
    );

    let globex = handle(
        &desk,
        "globex",
        Incident::new(
            "gx-7",
            "checkout",
            "Checkout pods crash looping again, out of memory after this morning's deploy",
        ),
    )
    .await;
    check(
        globex
            .similar
            .iter()
            .map(|m| m.id.as_str())
            .collect::<Vec<_>>()
            == ["inc-101"],
        "expected globex to recall only its own inc-101",
    );
    check(
        globex.action == "move the pods to another node pool",
        "expected globex's own fix, not acme's",
    );

    let unrelated = handle(
        &desk,
        "acme",
        Incident::new(
            "inc-203",
            "billing",
            "Billing emails delayed, the mail queue backlog keeps growing",
        ),
    )
    .await;
    check(
        unrelated.similar.is_empty() && unrelated.action == "page the on-call engineer",
        "expected nothing above 0.5 and an escalation",
    );

    // Without a tenant, a recall reads only tenant-less records: here, none.
    let no_tenant = memory
        .recall(
            "incidents",
            "Checkout pods crash looping, out of memory killed after deploy",
            RecallOptions::new(),
            None,
        )
        .await
        .expect("recall");
    println!(
        "  The same recall with no tenant (no run metadata): {} match(es)",
        no_tenant.len()
    );
    check(
        no_tenant.is_empty(),
        "expected a recall without a tenant to read no tenant's records",
    );

    // --- Step 3: the search's filters. ---

    println!();
    println!(
        "=== Step 3: filters -- metadata, minimum similarity, limit, excluded ids, namespace ==="
    );
    const QUERY: &str = "pods out of memory killed";
    let acme = || RecallOptions::new().tenant("acme");
    let all = recall(&memory, QUERY, acme(), "everything (limit 10)").await;
    let by_search = recall(
        &memory,
        QUERY,
        acme().with_metadata("service", "search"),
        "metadata service=search",
    )
    .await;
    let close = recall(
        &memory,
        QUERY,
        acme().min_similarity(0.5),
        "min_similarity 0.5",
    )
    .await;
    let top3 = recall(&memory, QUERY, acme().limit(3), "limit 3").await;
    let first_id = all[0].record.id.clone();
    let excluded = recall(
        &memory,
        QUERY,
        acme().exclude(first_id.clone()),
        &format!("exclude [{first_id}]"),
    )
    .await;
    let runbooks = memory
        .recall("runbooks", QUERY, acme(), None)
        .await
        .expect("recall");
    println!("  namespace 'runbooks': {}", lines(&runbooks));

    check(
        all.len() == 9,
        "expected acme's nine incidents (six past, three from the desk)",
    );
    check(
        all.windows(2).all(|w| w[0].similarity >= w[1].similarity),
        "expected the matches most similar first",
    );
    // inc-101 and inc-103 share exactly the same query words, so they tie,
    // and a tie is ordered by id.
    check(
        all[0].similarity == all[1].similarity && ids(&all)[..2] == ["inc-101", "inc-103"],
        "expected a tie ordered by id",
    );
    check(
        ids(&by_search) == ["inc-103"],
        "expected only the search service's incident",
    );
    check(
        !close.is_empty() && close.len() < all.len() && close.iter().all(|m| m.similarity >= 0.5),
        "expected min_similarity to drop the weaker matches",
    );
    check(
        ids(&top3) == ids(&all)[..3],
        "expected limit 3 to keep the three best",
    );
    check(
        ids(&excluded) == ids(&all)[1..],
        "expected exclude to drop that id and nothing else",
    );
    check(
        ids(&runbooks) == ["rb-oom"] && all.iter().all(|m| m.record.id != "rb-oom"),
        "expected namespaces never to cross",
    );

    // --- Step 4: vectors are labelled by model. ---

    println!();
    println!("=== Step 4: another model's memory reads none of these records ===");
    // The same words, the same buckets, the same vectors -- but another model name.
    let upgraded = Memory::new(
        store.clone(),
        Arc::new(BagOfWords::new("demo", "bow-256-v2")),
    );
    let v2 = upgraded
        .recall("incidents", QUERY, acme(), None)
        .await
        .expect("recall");
    println!(
        "  '{}' recall: {} match(es); '{label}' recall: {}",
        upgraded.label().expect("a label"),
        v2.len(),
        all.len()
    );
    check(
        v2.is_empty(),
        "expected a search with another model's label to find nothing, even with identical vectors",
    );

    // --- Step 5: an upsert replaces a record but keeps its creation time. ---

    println!();
    println!("=== Step 5: remembering inc-101 again 300 days later ===");
    advance(300);
    memory
        .remember(
            past(
                "acme",
                "inc-101",
                "checkout",
                "Checkout pods crash looping, out of memory killed after deploy",
                "a memory leak in the v2.3 image (postmortem PM-17)",
                "roll back the deploy",
            ),
            None,
        )
        .await
        .expect("remember");
    let updated = store
        .get(&MemoryKey::new("incidents", "acme", "inc-101"))
        .await
        .expect("get")
        .expect("inc-101 is there");
    let root_cause = updated
        .content
        .as_ref()
        .and_then(|c| c.get("root_cause"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    println!(
        "  created {}, updated {}, root cause: {root_cause}",
        day(updated.created_at),
        day(updated.updated_at)
    );
    check(
        updated.created_at == Some(start)
            && updated.updated_at == Some(start + TimeDelta::days(300)),
        "expected created_at kept and updated_at moved",
    );
    check(
        root_cause.contains("PM-17"),
        "expected the content replaced",
    );

    // --- Step 6: purge by age. ---

    println!();
    println!("=== Step 6: 400 days in, purge incidents not written for 365 days ===");
    advance(100);
    let cutoff = *now.lock().unwrap() - TimeDelta::days(365);
    let purged = store
        .purge(&MemoryPurge::new(cutoff).namespace("incidents"))
        .await
        .expect("purge");
    let left = async |namespace: &str, tenant: &str| -> Vec<String> {
        store
            .list(&MemoryQuery::new(namespace).tenant(tenant))
            .await
            .expect("list")
            .records
            .into_iter()
            .map(|r| r.id)
            .collect()
    };
    let acme_left = left("incidents", "acme").await;
    let globex_left = left("incidents", "globex").await;
    let runbooks_left = left("runbooks", "acme").await;
    println!(
        "  Purged {purged} record(s) last written before {}.",
        day(Some(cutoff))
    );
    println!(
        "  Left: acme incidents [{}], globex incidents [{}], acme runbooks [{}]",
        acme_left.join(", "),
        globex_left.join(", "),
        runbooks_left.join(", ")
    );
    check(
        purged == 10,
        "expected ten records purged: acme's 102-106 and 201-203, globex's inc-101 and gx-7",
    );
    check(
        acme_left == ["inc-101"],
        "expected the record updated at day 300 to survive",
    );
    check(
        globex_left.is_empty(),
        "expected the purge to cover every tenant when it names none",
    );
    check(
        runbooks_left == ["rb-oom"],
        "expected the purge to leave another namespace alone",
    );

    println!();
    println!(
        "=== Done: memories were recalled by meaning, kept apart by tenant, namespace and model, updated in place and purged by age. ==="
    );
}

/// Recall -> Diagnose -> Remember. Neither memory node names a tenant:
/// `Memory` reads it from the run's metadata `tenant`.
fn incident_desk(memory: Memory) -> CompiledGraph<Incident> {
    let recall_memory = memory.clone();
    StateGraph::<Incident>::new()
        .add_async_fallible_node("Recall", move |incident: Incident, ctx: NodeContext| {
            let memory = recall_memory.clone();
            async move {
                let options = RecallOptions::new()
                    .limit(3)
                    .min_similarity(0.5)
                    .exclude(incident.id.clone());
                let matches = memory
                    .recall("incidents", &incident.symptoms, options, &ctx)
                    .await?;
                Ok(Incident {
                    similar: matches.iter().map(Recalled::from).collect(),
                    ..incident
                })
            }
        })
        .add_node("Diagnose", |incident: Incident| {
            match incident.similar.first().cloned() {
                Some(closest) => Incident {
                    diagnosis: format!("likely {} (like {})", closest.root_cause, closest.id),
                    action: closest.action,
                    ..incident
                },
                None => Incident {
                    diagnosis: "no similar incident".to_string(),
                    action: "page the on-call engineer".to_string(),
                    ..incident
                },
            }
        })
        .add_async_fallible_node("Remember", move |incident: Incident, ctx: NodeContext| {
            let memory = memory.clone();
            async move {
                let entry =
                    MemoryEntry::new("incidents", incident.id.clone(), incident.symptoms.clone())
                        .with_content(content(&incident.diagnosis, &incident.action))
                        .with_metadata("service", incident.service.clone());
                memory.remember(entry, &ctx).await?;
                Ok(incident)
            }
        })
        .add_edge("Recall", "Diagnose")
        .add_edge("Diagnose", "Remember")
        .compile()
        .expect("the incident desk is a valid graph")
}

/// Runs the desk for one incident, in `tenant` (the run's metadata), prints
/// what it recalled and decided, and checks the run made two observed
/// embedding calls.
async fn handle(desk: &CompiledGraph<Incident>, tenant: &str, incident: Incident) -> Incident {
    let (observer, mut events) = RunEventChannel::new();
    let config = RunConfig::new()
        .with_observer(observer)
        .with_metadata("tenant", tenant);
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let thread = incident.id.clone();
    let handled = match desk.run_with(thread, incident, store, config).await {
        GraphExecutionResult::Completed(state) => state,
        GraphExecutionResult::Failed(error) => fail(&format!("the run failed: {error:?}")),
        _ => fail("expected the run to complete"),
    };
    let mut embeddings = 0;
    loop {
        match tokio::time::timeout(PATIENCE, events.recv()).await {
            Ok(Some(event)) => {
                if let RunEventKind::ModelCallStarted { operation, .. } = &event.kind
                    && operation == "embeddings"
                {
                    embeddings += 1;
                }
            }
            Ok(None) => break,
            Err(_) => fail("the run's events never ended with RunCompleted"),
        }
    }

    println!("  [{tenant}] {}: \"{}\"", handled.id, handled.symptoms);
    for found in &handled.similar {
        println!(
            "      recalled {} ({:.3}): {}",
            found.id, found.similarity, found.root_cause
        );
    }
    println!("      -> {}; action: {}", handled.diagnosis, handled.action);
    check(
        embeddings == 2,
        "expected two observed embedding calls per run: the recall and the remember",
    );
    handled
}

async fn recall(
    memory: &Memory,
    text: &str,
    options: RecallOptions,
    label: &str,
) -> Vec<MemoryMatch> {
    let matches = memory
        .recall("incidents", text, options, None)
        .await
        .expect("recall");
    println!("  {label:<26} {}", lines(&matches));
    matches
}

fn lines(matches: &[MemoryMatch]) -> String {
    matches
        .iter()
        .map(|m| format!("{} {:.3}", m.record.id, m.similarity))
        .collect::<Vec<_>>()
        .join(", ")
}

fn ids(matches: &[MemoryMatch]) -> Vec<&str> {
    matches.iter().map(|m| m.record.id.as_str()).collect()
}

fn day(at: Option<DateTime<Utc>>) -> String {
    at.map_or_else(
        || "never".to_string(),
        |at| at.format("%Y-%m-%d").to_string(),
    )
}

fn content(root_cause: &str, action: &str) -> Map<String, Value> {
    match json!({ "root_cause": root_cause, "action": action }) {
        Value::Object(map) => map,
        _ => unreachable!("a JSON object literal"),
    }
}

fn past(
    tenant: &str,
    id: &str,
    service: &str,
    symptoms: &str,
    root_cause: &str,
    action: &str,
) -> MemoryEntry {
    MemoryEntry::new("incidents", id, symptoms)
        .with_tenant(tenant)
        .with_content(content(root_cause, action))
        .with_metadata("service", service)
}

fn check(ok: bool, what: &str) {
    if !ok {
        fail(what);
    }
}

fn fail(what: &str) -> ! {
    eprintln!("check failed: {what}");
    std::process::exit(1);
}

/// A deterministic stand-in for an embedding model: a hashed bag of words.
/// Each word (lower-cased, stop words dropped, a crude suffix strip) adds 1 to
/// one of 256 buckets chosen by FNV-1a, which, unlike std's `DefaultHasher`,
/// is fixed by its definition and the same in every build and process. Texts
/// that share words point the same way, so cosine similarity ranks them as a
/// real model would, if far more crudely. The same algorithm as the .NET
/// twin's `BagOfWords`, so both make the same vectors.
struct BagOfWords {
    provider: &'static str,
    model: &'static str,
    calls: AtomicUsize,
}

impl BagOfWords {
    const DIMENSIONS: usize = 256;

    const STOP_WORDS: &[&str] = &[
        "the", "and", "after", "again", "this", "that", "was", "were", "with", "during", "keeps",
        "new", "from", "for", "out",
    ];

    fn new(provider: &'static str, model: &'static str) -> Self {
        Self {
            provider,
            model,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    /// The words of `text`: runs of `[a-z0-9]` after lower-casing, stemmed,
    /// longer than two letters, and not stop words.
    fn words(text: &str) -> impl Iterator<Item = String> {
        text.to_lowercase()
            .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
            .filter(|w| !w.is_empty())
            .map(Self::stem)
            .filter(|w| w.len() > 2 && !Self::STOP_WORDS.contains(&w.as_str()))
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn stem(word: &str) -> String {
        let stemmed = if word.len() > 5 && word.ends_with("ing") {
            &word[..word.len() - 3]
        } else if word.len() > 4 && word.ends_with("ed") {
            &word[..word.len() - 2]
        } else if word.len() > 3 && word.ends_with('s') {
            &word[..word.len() - 1]
        } else {
            word
        };
        stemmed.to_string()
    }

    fn bucket(word: &str) -> usize {
        let mut hash: u32 = 2_166_136_261;
        for byte in word.bytes() {
            hash = (hash ^ u32::from(byte)).wrapping_mul(16_777_619);
        }
        (hash % Self::DIMENSIONS as u32) as usize
    }

    fn vector(text: &str) -> (Vec<f32>, u64) {
        let mut vector = vec![0.0f32; Self::DIMENSIONS];
        let mut tokens = 0;
        for word in Self::words(text) {
            vector[Self::bucket(&word)] += 1.0;
            tokens += 1;
        }
        // An all-zero vector has no direction, and the store refuses it.
        if vector.iter().all(|x| *x == 0.0) {
            vector[0] = 1.0;
        }
        (vector, tokens)
    }
}

impl Embedder for BagOfWords {
    fn provider(&self) -> &str {
        self.provider
    }

    fn default_model(&self) -> Option<&str> {
        Some(self.model)
    }

    fn embed(
        &self,
        request: EmbeddingRequest,
    ) -> BoxFuture<'_, Result<EmbeddingResponse, ChatError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let mut tokens = 0;
            let vectors = request
                .inputs
                .iter()
                .map(|text| {
                    let (vector, count) = Self::vector(text);
                    tokens += count;
                    vector
                })
                .collect();
            Ok(EmbeddingResponse::new(vectors)
                .with_model(self.model)
                .with_usage(ModelUsage::new(Some(tokens), None)))
        })
    }
}
