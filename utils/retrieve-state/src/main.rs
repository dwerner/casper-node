use std::{env, path::Path};

use reqwest::Client;

use casper_node::types::JsonBlock;

use retrieve_state::get_block;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client = Client::new();

    let chain_download_path = env::current_dir()?.join(retrieve_state::CHAIN_DOWNLOAD_PATH);

    println!("Getting highest block...");
    let block: JsonBlock = get_block(&mut client, None).await?;

    println!("Downloading global state using highest block's state-root-hash...");
    retrieve_state::download_trie_by_keys(&mut client, block.header.state_root_hash).await?;

    let download_until_height = if Path::exists(chain_download_path.as_path()) {
        let existing_chain = walkdir::WalkDir::new(retrieve_state::CHAIN_DOWNLOAD_PATH);
        let mut highest_downloaded_block = 0;
        for entry in existing_chain {
            if let Some(filename) = entry?.file_name().to_str() {
                let split = filename.split('-').collect::<Vec<&str>>();
                if let ["block", height, _hash] = &split[..] {
                    let height: u64 = height.parse::<u64>()?;
                    highest_downloaded_block = highest_downloaded_block.max(height);
                }
            }
        }
        highest_downloaded_block + 1
    } else {
        0
    };
    println!(
        "Downloading blocks with deploys since height {}...",
        download_until_height
    );
    retrieve_state::download_blocks(&mut client, block.hash, download_until_height).await?;
    Ok(())
}
