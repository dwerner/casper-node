use std::env;

use lmdb::EnvironmentFlags;
use reqwest::Client;

use casper_node::types::JsonBlock;

use retrieve_state::offline;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client = Client::new();
    let lmdb_path = env::current_dir()?.join(retrieve_state::LMDB_PATH);
    let chain_download_path = env::current_dir()?.join(retrieve_state::CHAIN_DOWNLOAD_PATH);

    let engine_state =
        retrieve_state::offline::create_execution_engine(lmdb_path, EnvironmentFlags::NO_SYNC)?;
    let genesis_block: JsonBlock = retrieve_state::get_block(&mut client, None).await?;

    println!("Downloading genesis global state...");
    retrieve_state::download_genesis_global_state(&mut client, &engine_state, &genesis_block)
        .await?;

    let download_until_height = match offline::get_highest_block_downloaded(&chain_download_path)? {
        Some(highest_block_downloaded) => highest_block_downloaded + 1,
        _ => 0,
    };
    println!(
        "Downloading blocks with deploys since height {}...",
        download_until_height
    );
    let highest_block: JsonBlock = retrieve_state::get_block(&mut client, None).await?;
    let block_files = retrieve_state::download_blocks(
        &mut client,
        &chain_download_path,
        highest_block.hash,
        download_until_height,
    )
    .await?;

    retrieve_state::download_protocol_data_for_blocks(&mut client, &engine_state, &block_files)
        .await?;

    Ok(())
}
