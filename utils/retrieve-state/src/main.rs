use std::env;

use lmdb::EnvironmentFlags;
use reqwest::Client;

use casper_node::types::JsonBlock;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client = Client::new();
    let lmdb_path = env::current_dir()?.join(retrieve_state::LMDB_PATH);
    let chain_download_path = env::current_dir()?.join(retrieve_state::CHAIN_DOWNLOAD_PATH);

    let engine_state =
        retrieve_state::offline::create_execution_engine(lmdb_path, EnvironmentFlags::NO_SYNC)?;

    println!("Downloading genesis global state...");
    let highest_block: JsonBlock = retrieve_state::get_block(&mut client, None).await?;

    println!(
        "Downloading all blocks since height {}...",
        highest_block.header.height
    );
    let block_files =
        retrieve_state::download_blocks(&mut client, &chain_download_path, highest_block.hash, 0)
            .await?;

    let genesis_block = block_files.get(0).expect("should have genesis block");
    let genesis_block = retrieve_state::offline::read_block_file(genesis_block).await?;

    println!("Retrieving global state at genesis...");
    retrieve_state::download_genesis_global_state(&mut client, &engine_state, &genesis_block.block)
        .await?;

    println!("Retrieving protocol data...");
    retrieve_state::download_protocol_data_for_blocks(&mut client, &engine_state, &block_files)
        .await?;

    Ok(())
}
