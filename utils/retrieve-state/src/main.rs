use reqwest::Client;

use casper_node::types::JsonBlock;

use retrieve_state::get_block;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut client = Client::new();

    println!("Getting highest block...");
    let block: JsonBlock = get_block(&mut client, None).await?;

    println!("Downloading global state using highest block's state-root-hash...");
    retrieve_state::lmdb_copy_trie_by_keys(&mut client, block.header.state_root_hash).await?;

    println!("Downloading deploys since genesis...");
    retrieve_state::download_chain_to_disk(&mut client, block.hash, 0).await?;
    Ok(())
}
