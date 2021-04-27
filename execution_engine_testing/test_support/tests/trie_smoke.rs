use std::collections::HashMap;

use jsonrpc_lite::{JsonRpc, Params};
use reqwest::Client;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;

use casper_execution_engine::{
    shared::{
        additive_map::AdditiveMap,
        newtypes::{Blake2bHash, CorrelationId},
        stored_value::StoredValue,
        transform::Transform,
    },
    storage::global_state::{in_memory::InMemoryGlobalState, StateProvider},
};
use casper_node::{
    crypto::hash::Digest,
    types::{json_compatibility::StoredValue as JsonStoredValue, JsonBlock},
};
use casper_types::Key;
use std::convert::TryInto;

const RPC_SERVER: &str = "http://localhost:50101/rpc";

async fn get_block<'de, T, P>(client: &mut Client, params: P) -> Result<T, anyhow::Error>
where
    T: DeserializeOwned,
    P: Serialize,
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

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn in_memory_copy_trie_by_keys() -> Result<(), anyhow::Error> {
    let mut client = Client::new();

    let block: JsonBlock = get_block(&mut client, Option::<u8>::None).await?;

    let remote_state_root_hash: [u8; Digest::LENGTH] = block.header.state_root_hash.to_array();
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

    let keys: Vec<Key> = get_keys(&mut client, state_get_keys_with_prefix_args).await?;

    let global_state = InMemoryGlobalState::empty()?;

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
        let json_stored_value: JsonStoredValue = get_item(&mut client, params).await?;
        let stored_value: StoredValue = json_stored_value.try_into().unwrap();
        transforms.insert(key, Transform::Write(stored_value));
    }

    println!("transforms: {:#?}", transforms);

    let correlation_id = CorrelationId::new();

    let state_root = global_state
        .commit(correlation_id, initial_root_hash, transforms)
        .unwrap();

    assert_eq!(state_root, block.header.state_root_hash.into());

    Ok(())
}
