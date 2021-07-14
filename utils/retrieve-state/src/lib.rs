use std::{
    collections::HashMap,
    convert::TryInto,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use jsonrpc_lite::{JsonRpc, Params};
use lmdb::DatabaseFlags;
use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};
use walkdir::DirEntry;

use casper_execution_engine::{
    core::engine_state::{EngineConfig, EngineState},
    shared::{
        additive_map::AdditiveMap,
        newtypes::{Blake2bHash, CorrelationId},
        stored_value::StoredValue,
        transform::Transform,
    },
    storage::{
        global_state::{lmdb::LmdbGlobalState, StateProvider},
        protocol_data::ProtocolData,
        protocol_data_store::lmdb::LmdbProtocolDataStore,
        transaction_source::lmdb::LmdbEnvironment,
        trie_store::lmdb::LmdbTrieStore,
    },
};
use casper_node::{
    components::contract_runtime::ExecutionPreState,
    crypto::hash::Digest,
    rpcs::{
        chain::{BlockIdentifier, GetBlockParams},
        info::{GetDeployParams, GetProtocolDataParams},
    },
    types::{json_compatibility::StoredValue as JsonStoredValue, BlockHash, Deploy, JsonBlock},
};
use casper_types::Key;

// TODO: make these parameters
const RPC_SERVER: &str = "http://localhost:11101/rpc";
pub const LMDB_PATH: &str = "lmdb-data";
pub const CHAIN_DOWNLOAD_PATH: &str = "chain-download";
pub const DEFAULT_TEST_MAX_DB_SIZE: usize = 483_183_820_800; // 450 gb
pub const DEFAULT_TEST_MAX_READERS: u32 = 512;

pub async fn get_block<'de, T>(
    client: &mut Client,
    params: Option<GetBlockParams>,
) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
{
    let url = RPC_SERVER;
    let method = "chain_get_block";
    let params = Params::from(json!(params));
    let rpc_req = JsonRpc::request_with_params(12345, method, params);
    let response = client.post(url).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    let value = rpc_res.get_result().unwrap();
    let block = value.get("block").unwrap();
    let deserialized = serde_json::from_value(block.clone())?;
    Ok(deserialized)
}

pub async fn get_genesis_block<'de, T>(client: &mut Client) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
{
    let url = RPC_SERVER;
    let method = "chain_get_block";
    let params = Params::from(json!(Some(GetBlockParams {
        block_identifier: BlockIdentifier::Height(0),
    })));
    let rpc_req = JsonRpc::request_with_params(12345, method, params);
    let response = client.post(url).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    let value = rpc_res.get_result().unwrap();
    let block = value.get("block").unwrap();
    let deserialized = serde_json::from_value(block.clone())?;
    Ok(deserialized)
}

pub async fn get_protocol_data<'de, T, P>(
    client: &mut Client,
    params: P,
) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
    P: Serialize,
{
    let url = RPC_SERVER;
    let method = "info_get_protocol_data";
    let params = Params::from(json!(params));
    let rpc_req = JsonRpc::request_with_params(12345, method, params);
    let response = client.post(url).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    if let Some(error) = rpc_res.get_error() {
        return Err(anyhow::format_err!(error.clone()));
    }
    let value = rpc_res.get_result().unwrap();
    let keys = value.get("protocol_data").unwrap();
    let deserialized = serde_json::from_value(keys.clone())?;
    Ok(deserialized)
}

async fn get_keys<'de, T, P>(client: &mut Client, params: P) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
    P: Serialize,
{
    let url = RPC_SERVER;
    let method = "state_get_keys_with_prefix";
    let params = Params::from(json!(params));
    let rpc_req = JsonRpc::request_with_params(12345, method, params);
    let response = client.post(url).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    if let Some(error) = rpc_res.get_error() {
        return Err(anyhow::format_err!(error.clone()));
    }
    let value = rpc_res.get_result().unwrap();
    let keys = value.get("keys").unwrap();
    let deserialized = serde_json::from_value(keys.clone())?;
    Ok(deserialized)
}

async fn get_item<'de, T, P>(client: &mut Client, params: P) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
    P: Serialize,
{
    let url = RPC_SERVER;
    let method = "state_get_item";
    let params = Params::from(json!(params));
    let rpc_req = JsonRpc::request_with_params(12345, method, params);
    let response = client.post(url).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    let value = rpc_res.get_result().unwrap();
    let stored_value = value.get("stored_value").unwrap();
    let deserialized = serde_json::from_value(stored_value.clone())?;
    Ok(deserialized)
}

async fn get_deploy<'de, T>(
    client: &mut Client,
    params: GetDeployParams,
) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
{
    let url = RPC_SERVER;
    let method = "info_get_deploy";
    let params = Params::from(json!(params));
    let rpc_req = JsonRpc::request_with_params(12345, method, params);
    let response = client.post(url).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    if let Some(error) = rpc_res.get_error() {
        return Err(anyhow::format_err!(error.clone()));
    }
    let value = rpc_res.get_result().unwrap();
    // GetDeployResult?
    let stored_value = value.get("deploy").unwrap();
    let deserialized = serde_json::from_value(stored_value.clone())?;
    Ok(deserialized)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BlockWithDeploys {
    pub block: JsonBlock,
    pub transfers: Vec<Deploy>,
    pub deploys: Vec<Deploy>,
}

impl BlockWithDeploys {
    pub async fn save(&self, path: impl AsRef<Path>) -> Result<(), anyhow::Error> {
        let path = PathBuf::from(path.as_ref());
        let file_path = path.join(format!(
            "block-{:0>24}-{}.json",
            self.block.header.height,
            hex::encode(self.block.hash)
        ));
        let mut file = File::create(file_path).await?;
        let json = serde_json::to_string_pretty(self)?;
        file.write_all(json.as_bytes()).await?;
        Ok(())
    }
}

pub async fn download_block_with_deploys(
    client: &mut Client,
    block_hash: BlockHash,
) -> Result<BlockWithDeploys, anyhow::Error> {
    let block_identifier = BlockIdentifier::Hash(block_hash);
    let block: JsonBlock = get_block(client, Some(GetBlockParams { block_identifier })).await?;

    let mut transfers = Vec::new();
    for transfer_hash in block.transfer_hashes() {
        let transfer: Deploy = get_deploy(
            client,
            GetDeployParams {
                deploy_hash: *transfer_hash,
            },
        )
        .await?;
        transfers.push(transfer);
    }

    let mut deploys = Vec::new();
    for deploy_hash in block.deploy_hashes() {
        let deploy: Deploy = get_deploy(
            client,
            GetDeployParams {
                deploy_hash: *deploy_hash,
            },
        )
        .await?;
        deploys.push(deploy);
    }

    Ok(BlockWithDeploys {
        block,
        transfers,
        deploys,
    })
}

pub async fn download_blocks(
    client: &mut Client,
    chain_download_path: impl AsRef<Path>,
    mut block_hash: BlockHash,
    until_height: u64,
) -> Result<Vec<DirEntry>, anyhow::Error> {
    if !chain_download_path.as_ref().exists() {
        tokio::fs::create_dir_all(&chain_download_path).await?;
    }
    loop {
        let block_with_deploys = download_block_with_deploys(client, block_hash).await?;
        block_with_deploys.save(&chain_download_path).await?;

        if block_with_deploys.block.header.height == until_height {
            break;
        }
        block_hash = block_with_deploys.block.header.parent_hash;
    }
    Ok(offline::get_block_files(chain_download_path))
}

pub async fn download_trie_by_keys(
    client: &mut Client,
    engine_state: &EngineState<LmdbGlobalState>,
    state_root_hash: Digest,
) -> Result<(), anyhow::Error> {
    let remote_state_root_hash: [u8; Digest::LENGTH] = state_root_hash.to_array();
    let remote_state_root_hash_str: String = hex::encode(remote_state_root_hash);
    println!(
        "Found remote state root hash: {:?}",
        remote_state_root_hash_str
    );

    let state_get_keys_with_prefix_args: HashMap<String, String> = {
        let mut tmp = HashMap::new();
        tmp.insert(String::from("prefix"), String::new());
        tmp.insert(
            String::from("state_root_hash"),
            remote_state_root_hash_str.clone(),
        );
        tmp
    };

    let keys: Vec<Key> = get_keys(client, state_get_keys_with_prefix_args).await?;

    let initial_root_hash: Blake2bHash = engine_state.state.empty_root();

    let create_state_get_item_params = |key| {
        let mut tmp = HashMap::new();
        tmp.insert(String::from("key"), key);
        tmp.insert(
            String::from("state_root_hash"),
            remote_state_root_hash_str.clone(),
        );
        tmp
    };

    let mut transforms: AdditiveMap<Key, Transform> = AdditiveMap::new();

    for key in keys {
        let params = create_state_get_item_params(key.to_formatted_string());
        let json_stored_value: JsonStoredValue = get_item(client, params).await?;
        let stored_value: StoredValue = json_stored_value.try_into().unwrap();
        transforms.insert(key, Transform::Write(stored_value));
    }

    let correlation_id = CorrelationId::new();

    println!("Downloaded {} transforms", transforms.len());

    let state_root = engine_state
        .state
        .commit(correlation_id, initial_root_hash, transforms)
        .unwrap();

    println!("Checking that state root matches");

    assert_eq!(state_root, remote_state_root_hash.into());

    println!("Downloaded state root matches expected {:?}", state_root);

    Ok(())
}

/// Ensures we have all protocol data downloaded
pub async fn download_protocol_data_for_blocks(
    client: &mut Client,
    engine_state: &EngineState<LmdbGlobalState>,
    block_file_entries: &[DirEntry],
) -> Result<(), anyhow::Error> {
    for block_file_entry in block_file_entries.iter() {
        let BlockWithDeploys { block, .. } = offline::read_block_file(block_file_entry).await?;
        if !matches!(
            engine_state.get_protocol_data(block.header.protocol_version),
            Ok(Some(_)),
        ) {
            let maybe_protocol_data: Option<ProtocolData> = get_protocol_data(
                client,
                GetProtocolDataParams {
                    protocol_version: block.header.protocol_version,
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
    Ok(())
}

pub async fn download_genesis_global_state(
    client: &mut Client,
    engine_state: &EngineState<LmdbGlobalState>,
    genesis_block: &JsonBlock,
) -> Result<(), anyhow::Error> {
    // if we can't find this block in the trie, download it from a running node
    if !matches!(
        engine_state.read_trie(
            Default::default(),
            genesis_block.header.state_root_hash.into()
        ),
        Ok(Some(_))
    ) {
        download_trie_by_keys(client, engine_state, genesis_block.header.state_root_hash).await?;
    }
    Ok(())
}

pub mod offline {

    use lmdb::EnvironmentFlags;

    use super::*;

    pub fn get_highest_block_downloaded(
        chain_download_path: impl AsRef<Path>,
    ) -> Result<Option<u64>, anyhow::Error> {
        let highest = if chain_download_path.as_ref().exists() {
            let existing_chain = walkdir::WalkDir::new(CHAIN_DOWNLOAD_PATH);
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
            Some(highest_downloaded_block)
        } else {
            None
        };
        Ok(highest)
    }

    pub fn get_genesis_execution_prestate(genesis_block: &JsonBlock) -> ExecutionPreState {
        ExecutionPreState::new(
            genesis_block.header.state_root_hash,
            0,
            BlockHash::new(Digest::from([0u8; Digest::LENGTH])),
            Digest::from([0u8; Digest::LENGTH]),
        )
    }

    pub fn get_block_files(chain_path: impl AsRef<Path>) -> Vec<DirEntry> {
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
        block_files
    }

    pub fn create_execution_engine(
        lmdb_path: impl AsRef<Path>,
        environment_flags: EnvironmentFlags,
    ) -> Result<Arc<EngineState<LmdbGlobalState>>, anyhow::Error> {
        if lmdb_path.as_ref().join("data.lmdb").exists() {
            return Err(anyhow::anyhow!(
                "lmdb data file already exists at {}",
                lmdb_path.as_ref().display()
            ));
        }
        if !lmdb_path.as_ref().exists() {
            println!(
                "creating new lmdb data dir {}",
                lmdb_path.as_ref().display()
            );
            fs::create_dir_all(&lmdb_path)?;
        }

        fs::create_dir_all(&lmdb_path)?;
        let lmdb_environment = Arc::new(LmdbEnvironment::with_flags(
            &lmdb_path,
            DEFAULT_TEST_MAX_DB_SIZE,
            DEFAULT_TEST_MAX_READERS,
            environment_flags,
        )?);
        lmdb_environment.env().sync(true)?;

        let lmdb_trie_store = Arc::new(LmdbTrieStore::new(
            &lmdb_environment,
            None,
            DatabaseFlags::empty(),
        )?);
        let lmdb_protocol_data_store = Arc::new(LmdbProtocolDataStore::new(
            &lmdb_environment,
            None,
            DatabaseFlags::empty(),
        )?);
        let global_state =
            LmdbGlobalState::empty(lmdb_environment, lmdb_trie_store, lmdb_protocol_data_store)?;

        global_state.environment.env().sync(true)?;

        Ok(Arc::new(EngineState::new(
            global_state,
            EngineConfig::default(),
        )))
    }

    pub fn load_execution_engine(
        lmdb_path: impl AsRef<Path>,
        state_root_hash: Blake2bHash,
    ) -> Result<Arc<EngineState<LmdbGlobalState>>, anyhow::Error> {
        let lmdb_data_file = lmdb_path.as_ref().join("data.lmdb");
        if !lmdb_path.as_ref().join("data.lmdb").exists() {
            return Err(anyhow::anyhow!(
                "lmdb data file not found at: {}",
                lmdb_data_file.display()
            ));
        }

        let lmdb_environment = Arc::new(LmdbEnvironment::new(
            &lmdb_path,
            DEFAULT_TEST_MAX_DB_SIZE,
            DEFAULT_TEST_MAX_READERS,
        )?);
        let lmdb_trie_store = Arc::new(LmdbTrieStore::open(&lmdb_environment, None)?);
        let lmdb_protocol_data_store =
            Arc::new(LmdbProtocolDataStore::open(&lmdb_environment, None)?);
        let global_state = LmdbGlobalState::new(
            lmdb_environment,
            lmdb_trie_store,
            lmdb_protocol_data_store,
            state_root_hash,
        );
        Ok(Arc::new(EngineState::new(
            global_state,
            EngineConfig::default(),
        )))
    }

    pub async fn read_block_file(
        block_file_entry: &DirEntry,
    ) -> Result<BlockWithDeploys, anyhow::Error> {
        let mut file = File::open(block_file_entry.path()).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        Ok(serde_json::from_slice::<BlockWithDeploys>(&buffer)?)
    }
}
