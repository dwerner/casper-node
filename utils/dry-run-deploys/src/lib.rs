use std::time::Instant;

use casper_execution_engine::{
    self, core::engine_state::EngineState, storage::global_state::lmdb::LmdbGlobalState,
};
use casper_node::{
    components::contract_runtime::{operations, BlockAndExecutionEffects, ExecutionPreState},
    types::{Block, Deploy, FinalizedBlock, JsonBlock},
};

pub fn execute_json_block(
    engine_state: &EngineState<LmdbGlobalState>,
    json_block: JsonBlock,
    execution_pre_state: ExecutionPreState,
    deploys: Vec<Deploy>,
) -> Result<BlockAndExecutionEffects, anyhow::Error> {
    let block: Block = json_block.into();
    let protocol_version = block.protocol_version();
    let finalized_block = FinalizedBlock::from(block.clone());

    let start = Instant::now();
    let block_and_execution_effects = operations::execute_finalized_block(
        engine_state,
        None,
        protocol_version,
        execution_pre_state,
        finalized_block,
        deploys,
    )?;
    println!(
        "{}, {}, {}, {}",
        block.height(),
        block.transfer_hashes().len(),
        block.deploy_hashes().len(),
        (Instant::now() - start).as_millis()
    );

    engine_state.state.environment.env().sync(true)?;

    Ok(block_and_execution_effects)
}
