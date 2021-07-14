use std::env;

use casper_node::components::contract_runtime::ExecutionPreState;

use lmdb::EnvironmentFlags;
use reqwest::Client;
use retrieve_state::{offline, BlockWithDeploys};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let chain_path = env::current_dir()?
        .join("../retrieve-state")
        .join(retrieve_state::CHAIN_DOWNLOAD_PATH);

    let lmdb_path = env::current_dir()?
        //.join("../retrieve-state")
        .join(retrieve_state::LMDB_PATH);

    let block_files = offline::get_block_files(chain_path);

    let genesis_block = offline::read_block_file(&block_files[0]).await?;

    // let engine_state = offline::load_execution_engine(
    //     lmdb_path,
    //     genesis_block.block.header.state_root_hash.into(),
    // )?;

    let fast_options = EnvironmentFlags::NO_SYNC
        | EnvironmentFlags::NO_META_SYNC
        | EnvironmentFlags::NO_LOCK
        | EnvironmentFlags::WRITE_MAP;

    let engine_state = if false {
        offline::create_execution_engine(lmdb_path, EnvironmentFlags::empty())?
    } else {
        offline::create_execution_engine(lmdb_path, fast_options)?
    };

    // TODO: remove this network call block, this is supposed to be offline only
    // executing block file
    // Some("block-000000000000000000000000-8747e82a80e0d2c8356106c0d05d2322bed0c5bd0ffb47d045263d1e92ced9a4.
    // json") Executing block at height 0, with 0 transfers, 0 deploys
    // took 0 ms
    // executing block file
    // Some("block-000000000000000000000001-465ce367e98cf85bcd272c16dd2be9dcc27ab965da4417086a97808617743929.
    // json") Executing block at height 1, with 3 transfers, 0 deploys
    // Error: Root not found:
    // Blake2bHash(0xa6154f741548b55ecb662a248da2a1a249ac7288f02db9e3b49f02272ed1766f)
    if true {
        let mut client = Client::new();
        retrieve_state::download_genesis_global_state(
            &mut client,
            &engine_state,
            &genesis_block.block,
        )
        .await?;

        retrieve_state::download_protocol_data_for_blocks(&mut client, &engine_state, &block_files)
            .await?;
    }

    let mut execution_pre_state = offline::get_genesis_execution_prestate(&genesis_block.block);

    println!("block, transfer_count, deploy_count, execution_time_ms");
    for block_file_entry in block_files.iter() {
        let BlockWithDeploys {
            block,
            transfers,
            mut deploys,
        } = offline::read_block_file(block_file_entry).await?;
        deploys.extend(transfers);

        // println!(
        //     "executing block file {:?}",
        //     block_file_entry.file_name().to_str()
        // );
        let block_and_execution_effects = dry_run_deploys::execute_json_block(
            &engine_state,
            block,
            execution_pre_state,
            deploys,
        )?;
        let header = block_and_execution_effects.block.take_header();
        execution_pre_state = ExecutionPreState::from(&header);
    }
    Ok(())
}
