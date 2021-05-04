use std::{collections::HashMap, convert::TryInto, error, fmt};

use jsonrpc_lite::{JsonRpc, Params};
use reqwest::Client;
use serde_json::json;

use casper_execution_engine::{
    shared::{
        additive_map::AdditiveMap,
        newtypes::{Blake2bHash, CorrelationId},
        stored_value::StoredValue,
        transform::Transform,
    },
    storage::global_state::{in_memory::InMemoryGlobalState, CommitResult, StateProvider},
};
use casper_node::{
    crypto::hash::Digest,
    types::{json_compatibility::StoredValue as JsonStoredValue, JsonBlock},
};
use casper_types::Key;

// RPC constants
const RPC_SERVER: &str = "http://localhost:50101/rpc";
const METHOD_GET_BLOCK: &str = "chain_get_block";
const METHOD_GET_KEYS: &str = "state_get_keys_with_prefix";
const METHOD_GET_ITEM: &str = "state_get_item";
const FIELD_NAME_BLOCK: &str = "block";
const FIELD_NAME_KEYS: &str = "keys";
const FIELD_NAME_STORED_VALUE: &str = "stored_value";

// Parameter name constants
const STATE_ROOT_HASH: &str = "state_root_hash";
const PREFIX: &str = "prefix";
const KEY: &str = "key";

#[derive(Debug)]
enum Error {
    GetResult,
    GetField,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for Error {}

async fn get_block(client: &mut Client, params: Params) -> Result<JsonBlock, anyhow::Error> {
    let rpc_req = JsonRpc::request_with_params(12345, METHOD_GET_BLOCK, params);
    let response = client.post(RPC_SERVER).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    let value = rpc_res.get_result().ok_or(Error::GetResult)?;
    let block = value.get(FIELD_NAME_BLOCK).ok_or(Error::GetField)?;
    let deserialized = serde_json::from_value(block.to_owned())?;
    Ok(deserialized)
}

async fn get_keys(client: &mut Client, params: Params) -> Result<Vec<Key>, anyhow::Error> {
    let rpc_req = JsonRpc::request_with_params(12345, METHOD_GET_KEYS, params);
    let response = client.post(RPC_SERVER).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    let value = rpc_res.get_result().ok_or(Error::GetResult)?;
    let keys = value.get(FIELD_NAME_KEYS).ok_or(Error::GetField)?;
    let deserialized = serde_json::from_value(keys.to_owned())?;
    Ok(deserialized)
}

async fn get_item(client: &mut Client, params: Params) -> Result<JsonStoredValue, anyhow::Error> {
    let rpc_req = JsonRpc::request_with_params(12345, METHOD_GET_ITEM, params);
    let response = client.post(RPC_SERVER).json(&rpc_req).send().await?;
    let rpc_res: JsonRpc = response.json().await?;
    let value = rpc_res.get_result().ok_or(Error::GetResult)?;
    let stored_value = value.get(FIELD_NAME_STORED_VALUE).ok_or(Error::GetField)?;
    let deserialized = serde_json::from_value(stored_value.to_owned())?;
    Ok(deserialized)
}

async fn retrieve_state() -> Result<(), anyhow::Error> {
    let mut client = Client::new();
    let global_state = InMemoryGlobalState::empty()?;

    let get_block_params = Params::from(json!(Option::<u8>::None));

    let block: JsonBlock = get_block(&mut client, get_block_params).await?;

    let remote_state_root_hash: [u8; Digest::LENGTH] = block.state_root_hash().to_array();
    let remote_state_root_hash_str: String = hex::encode(remote_state_root_hash);

    let state_get_keys_with_prefix_args = {
        let mut tmp = HashMap::new();
        tmp.insert(String::from(PREFIX), String::new());
        tmp.insert(
            String::from(STATE_ROOT_HASH),
            remote_state_root_hash_str.clone(),
        );
        Params::from(json!(tmp))
    };

    let keys: Vec<Key> = get_keys(&mut client, state_get_keys_with_prefix_args).await?;

    let create_state_get_item_params = |key| {
        let mut tmp = HashMap::new();
        tmp.insert(String::from(KEY), key);
        tmp.insert(
            String::from(STATE_ROOT_HASH),
            remote_state_root_hash_str.clone(),
        );
        Params::from(json!(tmp))
    };

    let mut transforms: AdditiveMap<Key, Transform> = AdditiveMap::new();

    for key in keys {
        let params = create_state_get_item_params(key.to_formatted_string());
        let json_stored_value: JsonStoredValue = get_item(&mut client, params).await?;
        let stored_value: StoredValue = json_stored_value.try_into()?;
        transforms.insert(key, Transform::Write(stored_value));
    }

    let commit_result =
        global_state.commit(CorrelationId::new(), global_state.empty_root(), transforms)?;

    match commit_result {
        CommitResult::Success { state_root } => {
            let expected = Blake2bHash::from(remote_state_root_hash);
            assert_eq!(state_root, expected)
        }
        result => panic!("commit was not a success: {:?}", result),
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    retrieve_state().await.unwrap();
}
