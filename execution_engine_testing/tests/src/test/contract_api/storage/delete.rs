use assert_matches::assert_matches;

use casper_engine_test_support::{
    internal::{ExecuteRequestBuilder, InMemoryWasmTestBuilder, DEFAULT_RUN_GENESIS_REQUEST},
    DEFAULT_ACCOUNT_ADDR,
};
use casper_execution_engine::core::engine_state::QueryResult;
use casper_types::{runtime_args, Key, RuntimeArgs, U512};

const CONTRACT_WRITE_DELETE: &str = "write_delete.wasm";

const ARG_ENTRY_POINT: &str = "entry_point";
const ARG_NAME: &str = "name";
const ARG_VALUE: &str = "value";

const METHOD_WRITE: &str = "write";
const METHOD_DELETE: &str = "delete";

#[ignore]
#[test]
fn write_delete() {
    let mut builder = InMemoryWasmTestBuilder::default();

    builder.run_genesis(&DEFAULT_RUN_GENESIS_REQUEST);

    let value_key = "one";
    let value = U512::one();

    let write_request = ExecuteRequestBuilder::standard(
        *DEFAULT_ACCOUNT_ADDR,
        CONTRACT_WRITE_DELETE,
        runtime_args! {
            ARG_ENTRY_POINT => METHOD_WRITE,
            ARG_NAME => value_key,
            ARG_VALUE => value,
        },
    )
    .build();

    builder.exec(write_request).commit().expect_success();

    let actual_value: U512 = builder
        .query(
            None,
            Key::Account(*DEFAULT_ACCOUNT_ADDR),
            &[value_key.to_string()],
        )
        .expect("should have value")
        .as_cl_value()
        .expect("should be CLValue")
        .clone()
        .into_t()
        .expect("should cast CLValue to U512");

    assert_eq!(actual_value, value);

    let delete_request = ExecuteRequestBuilder::standard(
        *DEFAULT_ACCOUNT_ADDR,
        CONTRACT_WRITE_DELETE,
        runtime_args! {
            ARG_ENTRY_POINT => METHOD_DELETE,
            ARG_NAME => value_key,
        },
    )
    .build();

    builder.exec(delete_request).commit().expect_success();

    let query_result = builder.query_result(
        None,
        Key::Account(*DEFAULT_ACCOUNT_ADDR),
        &[value_key.to_string()],
    );

    assert_matches!(query_result, QueryResult::ValueNotFound(_))
}
