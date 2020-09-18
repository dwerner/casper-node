//! The set of JSON-RPCs which the API server handles.
//!
//! See https://github.com/CasperLabs/ceps/blob/master/text/0009-client-api.md#rpcs for info.

pub(super) mod account;
pub(super) mod chain;
pub(super) mod info;
pub(super) mod state;
pub(super) mod balance;

use std::str;

use futures::{future::BoxFuture, TryFutureExt};
use http::Response;
use hyper::Body;
use serde::Deserialize;
use warp::{
    filters::BoxedFilter,
    reject::{self, Reject},
    Filter,
};
use warp_json_rpc::{filters, Builder};

use super::{ApiRequest, ReactorEventT};
use crate::effect::EffectBuilder;

/// The URL path.
const RPC_API_PATH: &str = "rpc";

/// Error code returned if the JSON-RPC response indicates failure.
///
/// See https://www.jsonrpc.org/specification#error_object for details.
#[repr(i64)]
enum ErrorCode {
    ParseDeploy = 32000,
    ParseDeployHash = 32001,
    NoSuchDeploy = 32002,
    ParseBlockHash = 32003,
    NoSuchBlock = 32004,
    ParseQueryKey = 32005,
    QueryFailed = 32006,
    QueryFailedToExecute = 32007,
    MetricsNotAvailable = 32008,
    ParseGetBalanceKey = 32009,
    GetBalanceFailed = 32010,
    GetBalanceFailedToExecute = 32011,
}

#[derive(Debug)]
pub(super) struct Error(String);

impl Reject for Error {}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error(error.to_string())
    }
}

/// A trait for creating a JSON-RPC filter where the request is required to have "params".
pub(super) trait RpcWithParams {
    /// The JSON-RPC "method" name.
    const METHOD: &'static str;

    /// The JSON-RPC request's "params" type.
    type RequestParams: for<'de> Deserialize<'de> + Send + 'static;

    /// Creates the warp filter for this particular RPC.
    fn create_filter<REv: ReactorEventT>(
        effect_builder: EffectBuilder<REv>,
    ) -> BoxedFilter<(Response<Body>,)> {
        warp::path(RPC_API_PATH)
            .and(filters::json_rpc())
            .and(filters::method(Self::METHOD))
            .and(filters::params::<Self::RequestParams>())
            .and_then(
                move |response_builder: Builder, params: Self::RequestParams| {
                    Self::handle_request(effect_builder, response_builder, params)
                        .map_err(reject::custom)
                },
            )
            .boxed()
    }

    /// Handles the incoming RPC request.
    fn handle_request<REv: ReactorEventT>(
        effect_builder: EffectBuilder<REv>,
        response_builder: Builder,
        params: Self::RequestParams,
    ) -> BoxFuture<'static, Result<Response<Body>, Error>>;
}

/// A trait for creating a JSON-RPC filter where the request is not required to have "params".
pub(super) trait RpcWithoutParams {
    /// The JSON-RPC "method" name.
    const METHOD: &'static str;

    /// Creates the warp filter for this particular RPC.
    fn create_filter<REv: ReactorEventT>(
        effect_builder: EffectBuilder<REv>,
    ) -> BoxedFilter<(Response<Body>,)> {
        warp::path(RPC_API_PATH)
            .and(filters::json_rpc())
            .and(filters::method(Self::METHOD))
            .and_then(move |response_builder: Builder| {
                Self::handle_request(effect_builder, response_builder).map_err(reject::custom)
            })
            .boxed()
    }

    /// Handles the incoming RPC request.
    fn handle_request<REv: ReactorEventT>(
        effect_builder: EffectBuilder<REv>,
        response_builder: Builder,
    ) -> BoxFuture<'static, Result<Response<Body>, Error>>;
}

/// A trait for creating a JSON-RPC filter where the request may optionally have "params".
pub(super) trait RpcWithOptionalParams {
    /// The JSON-RPC "method" name.
    const METHOD: &'static str;

    /// The JSON-RPC request's "params" type.  This will be passed to the handler wrapped in an
    /// `Option`.
    type RequestParams: for<'de> Deserialize<'de> + Send + 'static;

    /// Creates the warp filter for this particular RPC.
    fn create_filter<REv: ReactorEventT>(
        effect_builder: EffectBuilder<REv>,
    ) -> BoxedFilter<(Response<Body>,)> {
        let with_params = warp::path(RPC_API_PATH)
            .and(filters::json_rpc())
            .and(filters::method(Self::METHOD))
            .and(filters::params::<Self::RequestParams>())
            .and_then(
                move |response_builder: Builder, params: Self::RequestParams| {
                    Self::handle_request(effect_builder, response_builder, Some(params))
                        .map_err(reject::custom)
                },
            );
        let without_params = warp::path(RPC_API_PATH)
            .and(filters::json_rpc())
            .and(filters::method(Self::METHOD))
            .and_then(move |response_builder: Builder| {
                Self::handle_request(effect_builder, response_builder, None).map_err(reject::custom)
            });
        with_params.or(without_params).unify().boxed()
    }

    /// Handles the incoming RPC request.
    fn handle_request<REv: ReactorEventT>(
        effect_builder: EffectBuilder<REv>,
        response_builder: Builder,
        params: Option<Self::RequestParams>,
    ) -> BoxFuture<'static, Result<Response<Body>, Error>>;
}
