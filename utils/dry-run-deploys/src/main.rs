use std::env;

use casper_execution_engine::storage::{global_state::StateProvider, protocol_data::ProtocolData};
use reqwest::Client;
use tokio::{fs::File, io::AsyncReadExt};
use walkdir::DirEntry;

use casper_node::{
    components::contract_runtime::ExecutionPreState, crypto::hash::Digest,
    rpcs::info::GetProtocolDataParams, types::BlockHash,
};

use retrieve_state::BlockWithDeploys;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let chain_path = env::current_dir()?
        .join("../retrieve-state")
        .join(retrieve_state::CHAIN_DOWNLOAD_PATH);

    println!("chain path: {}", chain_path.display());
    let mut block_files = walkdir::WalkDir::new(chain_path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let file_name = entry.file_name().to_str()?;
            let split = file_name.split('-').collect::<Vec<&str>>();
            if let ["block", _height, _hash] = &split[..] {
                Some(entry)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    block_files.sort_by_key(|entry| entry.file_name().to_str().unwrap().to_string());

    let engine_state = dry_run_deploys::create_execution_engine()?;

    let genesis_block_file_entry = &block_files[0];
    let BlockWithDeploys {
        block: genesis_block,
        ..
    } = read_block_file(genesis_block_file_entry).await?;

    // if we can't find this block in the trie, download it from a running node
    if !matches!(
        engine_state.read_trie(
            Default::default(),
            genesis_block.header.state_root_hash.into()
        ),
        Ok(Some(_))
    ) {
        let mut client = Client::new();
        retrieve_state::download_trie_by_keys(&mut client, genesis_block.header.state_root_hash)
            .await?;
    }

    // Ensure we have all protocol data downloaded
    for block_file_entry in block_files.iter() {
        let BlockWithDeploys { block, .. } = read_block_file(block_file_entry).await?;
        if !matches!(
            engine_state.get_protocol_data(block.header.protocol_version),
            Ok(Some(_)),
        ) {
            let mut client = Client::new();
            let maybe_protocol_data: Option<ProtocolData> = retrieve_state::get_protocol_data(
                &mut client,
                GetProtocolDataParams {
                    protocol_version: genesis_block.header.protocol_version,
                },
            )
            .await?;
            let protocol_data = maybe_protocol_data.unwrap_or_else(|| {
                panic!(
                    "unable to get protocol data for {}",
                    block.header.protocol_version
                )
            });

            engine_state
                .state
                .put_protocol_data(block.header.protocol_version, &protocol_data)?;
        }
    }

    let mut execution_pre_state = ExecutionPreState::new(
        genesis_block.header.state_root_hash,
        0,
        BlockHash::new(Digest::from([0u8; Digest::LENGTH])),
        Digest::from([0u8; Digest::LENGTH]),
    );
    for block_file_entry in block_files.iter() {
        let BlockWithDeploys {
            block,
            transfers,
            mut deploys,
        } = read_block_file(block_file_entry).await?;
        deploys.extend(transfers);

        println!(
            "executing block file {:?}",
            block_file_entry.file_name().to_str()
        );
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

async fn read_block_file(block_file_entry: &DirEntry) -> Result<BlockWithDeploys, anyhow::Error> {
    let mut file = File::open(block_file_entry.path()).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    Ok(serde_json::from_slice::<retrieve_state::BlockWithDeploys>(
        &buffer,
    )?)
}
