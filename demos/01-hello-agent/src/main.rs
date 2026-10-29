//! The smallest possible Strata agent: a single-node graph that reads
//! `name` from its state and writes a `greeting`. No tools, no streaming,
//! no interruption — just `StateGraph` -> `.compile()` -> `.run()`.

use std::sync::Arc;

use datalium_strata::checkpoint::GraphExecutionResult;
use datalium_strata::graph::StateGraph;
use datalium_strata::store::CheckpointStore;
use datalium_strata_memory::checkpoint::InMemoryCheckpointStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HelloState {
    name: String,
    greeting: Option<String>,
}

#[tokio::main]
async fn main() {
    let graph = StateGraph::<HelloState>::new()
        .add_node("Greet", |state: HelloState| HelloState {
            greeting: Some(format!("Hello, {}!", state.name)),
            ..state
        })
        .compile()
        .expect("graph is structurally valid");

    // `run` always takes a CheckpointStore. By default a run writes a
    // checkpoint only where it stops early (an interrupt, a wait, a failure,
    // a cancellation), and this one never does, so the store stays empty.
    let store: Arc<dyn CheckpointStore> = Arc::new(InMemoryCheckpointStore::new());
    let initial = HelloState {
        name: "World".to_string(),
        greeting: None,
    };

    match graph.run("hello-thread", initial, store).await {
        GraphExecutionResult::Completed(state) => {
            println!("{}", state.greeting.expect("Greet node always sets this"));
        }
        GraphExecutionResult::Failed(err) => panic!("graph execution failed: {err:?}"),
        _ => unreachable!("no interrupt boundary, wait or cancellation in this graph"),
    }
}
