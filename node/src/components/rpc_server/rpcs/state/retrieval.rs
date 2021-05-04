//! TODO

use std::collections::VecDeque;

use futures::{future::BoxFuture, FutureExt};
use http::Response;
use hyper::Body;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use warp_json_rpc::Builder;

use casper_execution_engine::{
    core::engine_state::query, shared::newtypes::Blake2bHash,
    storage::trie::merkle_proof::TrieMerkleProof,
};
use casper_types::{account::AccountHash, Key, ProtocolVersion};

use crate::{
    components::rpc_server::{
        rpcs::{docs::DocExample, Error, ErrorCode, RpcWithParams, RpcWithParamsExt},
        ReactorEventT,
    },
    crypto::hash::Digest,
    effect::{requests::RpcRequest, EffectBuilder},
    reactor::QueueKind,
    types::{json_compatibility::StoredValue, Block},
};

static GET_KEYS_WITH_PREFIX_EXAMPLE: GetKeysWithPrefix = GetKeysWithPrefix {};
static GET_KEYS_WITH_PREFIX_PARAMS_EXAMPLE: Lazy<GetKeysWithPrefixParams> =
    Lazy::new(|| GetKeysWithPrefixParams {
        state_root_hash: *Block::doc_example().header().state_root_hash(),
        prefix: String::from("00"),
    });
static GET_KEYS_WITH_PREFIX_RESULT_EXAMPLE: Lazy<GetKeysWithPrefixResult> =
    Lazy::new(|| GetKeysWithPrefixResult { keys: Vec::new() });

/// TODO
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GetKeysWithPrefix {}

impl RpcWithParams for GetKeysWithPrefix {
    const METHOD: &'static str = "state_get_keys_with_prefix";
    type RequestParams = GetKeysWithPrefixParams;
    type ResponseResult = GetKeysWithPrefixResult;
}

impl RpcWithParamsExt for GetKeysWithPrefix {
    fn handle_request<REv: ReactorEventT>(
        effect_builder: EffectBuilder<REv>,
        response_builder: Builder,
        params: Self::RequestParams,
        _api_version: ProtocolVersion,
    ) -> BoxFuture<'static, Result<Response<Body>, Error>> {
        async move {
            let state_root_hash: Digest = params.state_root_hash;

            let prefix: Vec<u8> = match hex::decode(params.prefix) {
                Ok(prefix) => prefix,
                Err(error) => {
                    let error_msg = format!("failed to parse prefix: {}", error);
                    return Ok(response_builder.error(warp_json_rpc::Error::custom(
                        ErrorCode::ParseGetKeysPrefix as i64,
                        error_msg,
                    ))?);
                }
            };

            let get_keys_result = effect_builder
                .make_request(
                    |responder| RpcRequest::GetKeysWithPrefix {
                        state_root_hash,
                        prefix,
                        responder,
                    },
                    QueueKind::Api,
                )
                .await;

            let keys: Vec<Key> = match get_keys_result {
                Ok(query::GetKeysWithPrefixResult::Success { keys }) => keys,
                Ok(query::GetKeysWithPrefixResult::RootNotFound) => todo!(),
                Err(_) => todo!(),
            };

            let result = Self::ResponseResult { keys };

            Ok(response_builder.success(result)?)
        }
        .boxed()
    }
}

impl DocExample for GetKeysWithPrefix {
    fn doc_example() -> &'static Self {
        &GET_KEYS_WITH_PREFIX_EXAMPLE
    }
}

/// TODO
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GetKeysWithPrefixParams {
    /// TODO
    pub state_root_hash: Digest,
    /// TODO
    pub prefix: String,
}

impl DocExample for GetKeysWithPrefixParams {
    fn doc_example() -> &'static Self {
        &*GET_KEYS_WITH_PREFIX_PARAMS_EXAMPLE
    }
}

/// TODO
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GetKeysWithPrefixResult {
    /// TODO
    #[schemars(with = "String", description = "List of keys")]
    pub keys: Vec<Key>,
}

impl DocExample for GetKeysWithPrefixResult {
    fn doc_example() -> &'static Self {
        &*GET_KEYS_WITH_PREFIX_RESULT_EXAMPLE
    }
}

static READ_EXAMPLE: Read = Read {};

/// TODO
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Read {}

impl RpcWithParams for Read {
    const METHOD: &'static str = "state_read";
    type RequestParams = ReadParams;
    type ResponseResult = ReadResult;
}

impl DocExample for Read {
    fn doc_example() -> &'static Self {
        &READ_EXAMPLE
    }
}

/// TODO
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ReadParams {
    /// TODO
    #[schemars(with = "String", description = "Hex encoded blake2b hash.")]
    pub state_root_hash: Blake2bHash,
}

static READ_PARAMS_EXAMPLE: Lazy<ReadParams> = Lazy::new(|| ReadParams {
    state_root_hash: Blake2bHash::new(&[]),
});

static READ_RESULT_EXAMPLE: Lazy<ReadResult> = Lazy::new(|| ReadResult {
    proofs: TrieMerkleProof::new(
        Key::Account(
            AccountHash::from_formatted_str(
                "accounthash-0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        ),
        StoredValue::ContractWasm("wasm bytes".to_string()),
        VecDeque::new(),
    ),
});

/// TODO
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct ReadResult {
    #[schemars(with = "String", description = "Trie Merkle Proof. 1.")]
    proofs: TrieMerkleProof<Key, StoredValue>,
}

/// TODO
#[allow(unused)]
static TRIE_MERKLE_PROOF_EXAMPLE: Lazy<TrieMerkleProof<Key, StoredValue>> = Lazy::new(|| {
    TrieMerkleProof::new(
        Key::Account(AccountHash::from_formatted_str("deadbeef").unwrap()),
        StoredValue::ContractWasm("wasm_bytes".to_string()),
        VecDeque::new(),
    )
});

impl DocExample for ReadParams {
    fn doc_example() -> &'static Self {
        &*READ_PARAMS_EXAMPLE
    }
}

impl DocExample for ReadResult {
    fn doc_example() -> &'static Self {
        &*READ_RESULT_EXAMPLE
    }
}
