use std::{
    collections::HashMap,
    convert::TryInto,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use jsonrpc_lite::{JsonRpc, Params};
use lmdb::DatabaseFlags;
use reqwest::Client;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};

use casper_execution_engine::{
    shared::{
        additive_map::AdditiveMap,
        newtypes::{Blake2bHash, CorrelationId},
        stored_value::StoredValue,
        transform::Transform,
    },
    storage::{
        global_state::{lmdb::LmdbGlobalState, StateProvider},
        protocol_data_store::lmdb::LmdbProtocolDataStore,
        transaction_source::lmdb::LmdbEnvironment,
        trie_store::lmdb::LmdbTrieStore,
    },
};
use casper_node::{
    crypto::hash::Digest,
    rpcs::{
        chain::{BlockIdentifier, GetBlockParams},
        info::GetDeployParams,
    },
    types::{json_compatibility::StoredValue as JsonStoredValue, BlockHash, Deploy, JsonBlock},
};
use casper_types::Key;

const RPC_SERVER: &str = "http://localhost:11101/rpc";
const LMDB_PATH: &str = "lmdb_data";
const CHAIN_DOWNLOAD_PATH: &str = "chain-download";
const DEFAULT_TEST_MAX_DB_SIZE: usize = 52_428_800; // 50 MiB
const DEFAULT_TEST_MAX_READERS: u32 = 512;

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
    let method = "get_deploy";
    let params = Params::from(json!(params));
    let rpc_req = JsonRpc::request_with_params(12345, method, params);
    let response = client.post(url).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    let value = rpc_res.get_result().unwrap();
    let stored_value = value.get("deploy").unwrap();
    let deserialized = serde_json::from_value(stored_value.clone())?;
    Ok(deserialized)
}

#[derive(Serialize)]
pub struct BlockWithDeploys {
    block: JsonBlock,
    transfers: Vec<Deploy>,
    deploys: Vec<Deploy>,
}

impl BlockWithDeploys {
    pub async fn save(&self, path: PathBuf) -> Result<(), anyhow::Error> {
        let file_path = path.join(format!(
            "block-{}-{}.json",
            self.block.header.height,
            hex::encode(self.block.hash)
        ));
        let mut writer = BufWriter::new(File::create(file_path).await?);
        let json = serde_json::to_vec(self)?;
        writer.write_all(&json).await?;
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

pub async fn download_chain_to_disk(
    client: &mut Client,
    mut block_hash: BlockHash,
    until_height: u64,
) -> Result<(), anyhow::Error> {
    // go back by parent hashes, to genesis
    let chain_download_path = env::current_dir()?.join(CHAIN_DOWNLOAD_PATH);
    tokio::fs::create_dir_all(chain_download_path).await?;
    loop {
        let block_with_deploys = download_block_with_deploys(client, block_hash).await?;
        block_with_deploys.save(CHAIN_DOWNLOAD_PATH.into()).await?;

        if block_with_deploys.block.header.height == until_height {
            break;
        }
        block_hash = block_with_deploys.block.header.parent_hash;
    }
    Ok(())
}

pub async fn lmdb_copy_trie_by_keys(
    client: &mut Client,
    state_root_hash: Digest,
) -> Result<(), anyhow::Error> {
    let remote_state_root_hash: [u8; Digest::LENGTH] = state_root_hash.to_array();
    let remote_state_root_hash_str: String = hex::encode(remote_state_root_hash);
    println!(
        "remote_state_root_hash_str: {:?}",
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

    let lmdb_path = env::current_dir()?.join(LMDB_PATH);
    if !Path::exists(&lmdb_path) {
        fs::create_dir_all(&lmdb_path)?;
    }
    let lmdb_environment = LmdbEnvironment::new(
        &lmdb_path,
        DEFAULT_TEST_MAX_DB_SIZE,
        DEFAULT_TEST_MAX_READERS,
    )?;
    let lmdb_environment = Arc::new(lmdb_environment);
    let lmdb_trie_store = LmdbTrieStore::new(&lmdb_environment, None, DatabaseFlags::empty())?;
    let lmdb_trie_store = Arc::new(lmdb_trie_store);
    let lmdb_protocol_data_store =
        LmdbProtocolDataStore::new(&lmdb_environment, None, DatabaseFlags::empty())?;
    let lmdb_protocol_data_store = Arc::new(lmdb_protocol_data_store);
    let global_state =
        LmdbGlobalState::empty(lmdb_environment, lmdb_trie_store, lmdb_protocol_data_store)?;

    let initial_root_hash: Blake2bHash = global_state.empty_root();

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

    println!("downloaded {} transforms", transforms.len());

    let state_root = global_state
        .commit(correlation_id, initial_root_hash, transforms)
        .unwrap();

    println!("checking that state root matches");
    assert_eq!(state_root, remote_state_root_hash.into());
    println!("downloaded state root matches expected {:?}", state_root);

    Ok(())
}
