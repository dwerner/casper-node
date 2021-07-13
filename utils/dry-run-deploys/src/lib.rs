use std::{env, fs, path::Path, sync::Arc};

use casper_execution_engine::{
    core::engine_state::{EngineConfig, EngineState},
    storage::{
        global_state::lmdb::LmdbGlobalState, protocol_data_store::lmdb::LmdbProtocolDataStore,
        transaction_source::lmdb::LmdbEnvironment, trie_store::lmdb::LmdbTrieStore,
    },
};
use casper_node::{
    components::contract_runtime::{
        operations, BlockAndExecutionEffects, BlockExecutionError, ExecutionPreState,
    },
    types::{Block, Deploy, FinalizedBlock, JsonBlock},
};
use lmdb::DatabaseFlags;

pub fn create_execution_engine() -> Result<Arc<EngineState<LmdbGlobalState>>, anyhow::Error> {
    let lmdb_path = env::current_dir()?.join(retrieve_state::LMDB_PATH);

    println!("lmdb_path {:?}", lmdb_path);

    if !Path::exists(&lmdb_path) {
        fs::create_dir_all(&lmdb_path)?;
    }

    let lmdb_environment = LmdbEnvironment::new(
        &lmdb_path,
        retrieve_state::DEFAULT_TEST_MAX_DB_SIZE,
        retrieve_state::DEFAULT_TEST_MAX_READERS,
    )?;

    let lmdb_environment = Arc::new(lmdb_environment);
    let lmdb_trie_store = LmdbTrieStore::new(&lmdb_environment, None, DatabaseFlags::empty())?;
    let lmdb_trie_store = Arc::new(lmdb_trie_store);
    let lmdb_protocol_data_store =
        LmdbProtocolDataStore::new(&lmdb_environment, None, DatabaseFlags::empty())?;
    let lmdb_protocol_data_store = Arc::new(lmdb_protocol_data_store);
    let global_state =
        LmdbGlobalState::empty(lmdb_environment, lmdb_trie_store, lmdb_protocol_data_store)?;
    let engine_state = EngineState::new(global_state, EngineConfig::default());

    Ok(Arc::new(engine_state))
}

pub fn execute_json_block(
    engine_state: &EngineState<LmdbGlobalState>,
    json_block: JsonBlock,
    execution_pre_state: ExecutionPreState,
    deploys: Vec<Deploy>,
) -> Result<BlockAndExecutionEffects, BlockExecutionError> {
    let block: Block = json_block.into();
    let protocol_version = block.protocol_version();
    let finalized_block = FinalizedBlock::from(block);

    let block_and_execution_effects = operations::execute_finalized_block(
        engine_state,
        None,
        protocol_version,
        execution_pre_state,
        finalized_block,
        deploys,
    )?;

    Ok(block_and_execution_effects)
}
