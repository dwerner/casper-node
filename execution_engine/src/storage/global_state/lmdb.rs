use std::{ops::Deref, sync::Arc};

use crate::shared::{
    additive_map::AdditiveMap,
    newtypes::{Blake2bHash, CorrelationId},
    stored_value::StoredValue,
    transform::Transform,
};
use casper_types::{Key, ProtocolVersion};

use crate::storage::{
    error,
    global_state::{commit, CommitResult, StateProvider, StateReader},
    protocol_data::ProtocolData,
    protocol_data_store::lmdb::LmdbProtocolDataStore,
    store::Store,
    transaction_source::{lmdb::LmdbEnvironment, Transaction, TransactionSource},
    trie::{merkle_proof::TrieMerkleProof, operations::create_hashed_empty_trie, Trie},
    trie_store::{
        lmdb::LmdbTrieStore,
        operations::{missing_descendant_trie_keys, put_trie, read, read_with_proof, ReadResult},
    },
};

pub struct LmdbGlobalState {
    pub environment: Arc<LmdbEnvironment>,
    pub trie_store: Arc<LmdbTrieStore>,
    pub protocol_data_store: Arc<LmdbProtocolDataStore>,
    pub empty_root_hash: Blake2bHash,
}

/// Represents a "view" of global state at a particular root hash.
pub struct LmdbGlobalStateView {
    pub environment: Arc<LmdbEnvironment>,
    pub store: Arc<LmdbTrieStore>,
    pub root_hash: Blake2bHash,
}

impl LmdbGlobalState {
    /// Creates an empty state from an existing environment and trie_store.
    pub fn empty(
        environment: Arc<LmdbEnvironment>,
        trie_store: Arc<LmdbTrieStore>,
        protocol_data_store: Arc<LmdbProtocolDataStore>,
    ) -> Result<Self, error::Error> {
        let root_hash: Blake2bHash = {
            let (root_hash, root) = create_hashed_empty_trie::<Key, StoredValue>()?;
            let mut txn = environment.create_read_write_txn()?;
            trie_store.put(&mut txn, &root_hash, &root)?;
            txn.commit()?;
            root_hash
        };
        Ok(LmdbGlobalState::new(
            environment,
            trie_store,
            protocol_data_store,
            root_hash,
        ))
    }

    /// Creates a state from an existing environment, store, and root_hash.
    /// Intended to be used for testing.
    pub(crate) fn new(
        environment: Arc<LmdbEnvironment>,
        trie_store: Arc<LmdbTrieStore>,
        protocol_data_store: Arc<LmdbProtocolDataStore>,
        empty_root_hash: Blake2bHash,
    ) -> Self {
        LmdbGlobalState {
            environment,
            trie_store,
            protocol_data_store,
            empty_root_hash,
        }
    }
}

impl StateReader<Key, StoredValue> for LmdbGlobalStateView {
    type Error = error::Error;

    fn read(
        &self,
        correlation_id: CorrelationId,
        key: &Key,
    ) -> Result<Option<StoredValue>, Self::Error> {
        let txn = self.environment.create_read_txn()?;
        let ret = match read::<Key, StoredValue, lmdb::RoTransaction, LmdbTrieStore, Self::Error>(
            correlation_id,
            &txn,
            self.store.deref(),
            &self.root_hash,
            key,
        )? {
            ReadResult::Found(value) => Some(value),
            ReadResult::NotFound => None,
            ReadResult::RootNotFound => panic!("LmdbGlobalState has invalid root"),
        };
        txn.commit()?;
        Ok(ret)
    }

    fn read_with_proof(
        &self,
        correlation_id: CorrelationId,
        key: &Key,
    ) -> Result<Option<TrieMerkleProof<Key, StoredValue>>, Self::Error> {
        let txn = self.environment.create_read_txn()?;
        let ret = match read_with_proof::<
            Key,
            StoredValue,
            lmdb::RoTransaction,
            LmdbTrieStore,
            Self::Error,
        >(
            correlation_id,
            &txn,
            self.store.deref(),
            &self.root_hash,
            key,
        )? {
            ReadResult::Found(value) => Some(value),
            ReadResult::NotFound => None,
            ReadResult::RootNotFound => panic!("LmdbGlobalState has invalid root"),
        };
        txn.commit()?;
        Ok(ret)
    }

    /// Reads a `Trie<K,V>` from the state if it is present
    fn read_trie(
        &self,
        _correlation_id: CorrelationId,
        trie_key: &Blake2bHash,
    ) -> Result<Option<Trie<Key, StoredValue>>, Self::Error> {
        let txn = self.environment.create_read_txn()?;
        let ret: Option<Trie<Key, StoredValue>> = self.store.get(&txn, trie_key)?;
        txn.commit()?;
        Ok(ret)
    }
}

impl StateProvider for LmdbGlobalState {
    type Error = error::Error;

    type Reader = LmdbGlobalStateView;

    fn checkout(&self, state_hash: Blake2bHash) -> Result<Option<Self::Reader>, Self::Error> {
        let txn = self.environment.create_read_txn()?;
        let maybe_root: Option<Trie<Key, StoredValue>> = self.trie_store.get(&txn, &state_hash)?;
        let maybe_state = maybe_root.map(|_| LmdbGlobalStateView {
            environment: Arc::clone(&self.environment),
            store: Arc::clone(&self.trie_store),
            root_hash: state_hash,
        });
        txn.commit()?;
        Ok(maybe_state)
    }

    fn commit(
        &self,
        correlation_id: CorrelationId,
        prestate_hash: Blake2bHash,
        effects: AdditiveMap<Key, Transform>,
    ) -> Result<CommitResult, Self::Error> {
        let commit_result = commit::<LmdbEnvironment, LmdbTrieStore, _, Self::Error>(
            &self.environment,
            &self.trie_store,
            correlation_id,
            prestate_hash,
            effects,
        )?;
        Ok(commit_result)
    }

    fn put_protocol_data(
        &self,
        protocol_version: ProtocolVersion,
        protocol_data: &ProtocolData,
    ) -> Result<(), Self::Error> {
        let mut txn = self.environment.create_read_write_txn()?;
        self.protocol_data_store
            .put(&mut txn, &protocol_version, protocol_data)?;
        txn.commit().map_err(Into::into)
    }

    fn get_protocol_data(
        &self,
        protocol_version: ProtocolVersion,
    ) -> Result<Option<ProtocolData>, Self::Error> {
        let txn = self.environment.create_read_txn()?;
        let result = self.protocol_data_store.get(&txn, &protocol_version)?;
        txn.commit()?;
        Ok(result)
    }

    fn empty_root(&self) -> Blake2bHash {
        self.empty_root_hash
    }

    fn put_trie(
        &self,
        correlation_id: CorrelationId,
        trie: &Trie<Key, StoredValue>,
    ) -> Result<(), Self::Error> {
        let mut txn = self.environment.create_read_write_txn()?;
        put_trie::<Key, StoredValue, lmdb::RwTransaction, LmdbTrieStore, Self::Error>(
            correlation_id,
            &mut txn,
            &self.trie_store,
            trie,
        )?;
        txn.commit()?;
        Ok(())
    }

    /// Finds all of the keys of missing descendant `Trie<K,V>` values
    fn missing_descendant_trie_keys(
        &self,
        correlation_id: CorrelationId,
        trie_key: Blake2bHash,
    ) -> Result<Vec<Blake2bHash>, Self::Error> {
        let txn = self.environment.create_read_txn()?;
        let missing_descendants =
            missing_descendant_trie_keys::<
                Key,
                StoredValue,
                lmdb::RoTransaction,
                LmdbTrieStore,
                Self::Error,
            >(correlation_id, &txn, self.trie_store.deref(), trie_key)?;
        txn.commit()?;
        Ok(missing_descendants)
    }
}

#[cfg(test)]
mod tests {
    use lmdb::DatabaseFlags;
    use tempfile::tempdir;

    use crate::shared::newtypes::Blake2bHash;
    use casper_types::{account::AccountHash, bytesrepr::ToBytes, CLValue};

    use super::*;
    use crate::storage::{
        trie_store::{
            operations,
            operations::{write, WriteResult},
        },
        DEFAULT_TEST_MAX_DB_SIZE, DEFAULT_TEST_MAX_READERS,
    };

    #[derive(Debug, Clone)]
    struct TestPair {
        key: Key,
        value: StoredValue,
    }

    fn create_test_pairs() -> [TestPair; 3] {
        [
            TestPair {
                key: Key::Account(AccountHash::new([1_u8; 32])),
                value: StoredValue::CLValue(CLValue::from_t(1_i32).unwrap()),
            },
            TestPair {
                key: Key::Account(AccountHash::new([2_u8; 32])),
                value: StoredValue::CLValue(CLValue::from_t(2_i32).unwrap()),
            },
            TestPair {
                key: Key::Account(AccountHash::new(
                    [2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8,
                           1_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8,
                        // ^^^^ Is 1_u8 not 2_u8! (makes an extension node to pointer not leaf)
                           2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8,
                           2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, 2_u8, ])),
                value: StoredValue::CLValue(CLValue::from_t(2_i32).unwrap()),
            },
        ]
    }

    fn create_test_pairs_updated() -> [TestPair; 3] {
        [
            TestPair {
                key: Key::Account(AccountHash::new([1u8; 32])),
                value: StoredValue::CLValue(CLValue::from_t("one".to_string()).unwrap()),
            },
            TestPair {
                key: Key::Account(AccountHash::new([2u8; 32])),
                value: StoredValue::CLValue(CLValue::from_t("two".to_string()).unwrap()),
            },
            TestPair {
                key: Key::Account(AccountHash::new([3u8; 32])),
                value: StoredValue::CLValue(CLValue::from_t(3_i32).unwrap()),
            },
        ]
    }

    fn new_empty_lmdb_global_state() -> LmdbGlobalState {
        let temp_dir = tempdir().unwrap();
        let environment = Arc::new(
            LmdbEnvironment::new(
                &temp_dir.path().to_path_buf(),
                DEFAULT_TEST_MAX_DB_SIZE,
                DEFAULT_TEST_MAX_READERS,
            )
            .unwrap(),
        );
        let trie_store =
            Arc::new(LmdbTrieStore::new(&environment, None, DatabaseFlags::empty()).unwrap());
        let protocol_data_store = Arc::new(
            LmdbProtocolDataStore::new(&environment, None, DatabaseFlags::empty()).unwrap(),
        );
        LmdbGlobalState::empty(environment, trie_store, protocol_data_store).unwrap()
    }

    fn create_test_state() -> (LmdbGlobalState, Blake2bHash) {
        let correlation_id = CorrelationId::new();
        let ret = new_empty_lmdb_global_state();
        let mut current_root = ret.empty_root_hash;
        {
            let mut txn = ret.environment.create_read_write_txn().unwrap();

            for TestPair { key, value } in &create_test_pairs() {
                match write::<_, _, _, LmdbTrieStore, error::Error>(
                    correlation_id,
                    &mut txn,
                    &ret.trie_store,
                    &current_root,
                    key,
                    value,
                )
                .unwrap()
                {
                    WriteResult::Written(root_hash) => {
                        current_root = root_hash;
                    }
                    WriteResult::AlreadyExists => (),
                    WriteResult::RootNotFound => panic!("LmdbGlobalState has invalid root"),
                }
            }

            txn.commit().unwrap();
        }
        (ret, current_root)
    }

    #[test]
    fn reads_from_a_checkout_return_expected_values() {
        let correlation_id = CorrelationId::new();
        let (state, root_hash) = create_test_state();
        let checkout = state.checkout(root_hash).unwrap().unwrap();
        for TestPair { key, value } in create_test_pairs().iter().cloned() {
            assert_eq!(Some(value), checkout.read(correlation_id, &key).unwrap());
        }
    }

    #[test]
    fn checkout_fails_if_unknown_hash_is_given() {
        let (state, _) = create_test_state();
        let fake_hash: Blake2bHash = Blake2bHash::new(&[1u8; 32]);
        let result = state.checkout(fake_hash).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn commit_updates_state() {
        let correlation_id = CorrelationId::new();
        let test_pairs_updated = create_test_pairs_updated();

        let (state, root_hash) = create_test_state();

        let effects: AdditiveMap<Key, Transform> = {
            let mut tmp = AdditiveMap::new();
            for TestPair { key, value } in &test_pairs_updated {
                tmp.insert(*key, Transform::Write(value.to_owned()));
            }
            tmp
        };

        let updated_hash = match state.commit(correlation_id, root_hash, effects).unwrap() {
            CommitResult::Success { state_root, .. } => state_root,
            _ => panic!("commit failed"),
        };

        let updated_checkout = state.checkout(updated_hash).unwrap().unwrap();

        for TestPair { key, value } in test_pairs_updated.iter().cloned() {
            assert_eq!(
                Some(value),
                updated_checkout.read(correlation_id, &key).unwrap()
            );
        }
    }

    #[test]
    fn commit_updates_state_and_original_state_stays_intact() {
        let correlation_id = CorrelationId::new();
        let test_pairs_updated = create_test_pairs_updated();

        let (state, root_hash) = create_test_state();

        let effects: AdditiveMap<Key, Transform> = {
            let mut tmp = AdditiveMap::new();
            for TestPair { key, value } in &test_pairs_updated {
                tmp.insert(*key, Transform::Write(value.to_owned()));
            }
            tmp
        };

        let updated_hash = match state.commit(correlation_id, root_hash, effects).unwrap() {
            CommitResult::Success { state_root, .. } => state_root,
            _ => panic!("commit failed"),
        };

        let updated_checkout = state.checkout(updated_hash).unwrap().unwrap();
        for TestPair { key, value } in test_pairs_updated.iter().cloned() {
            assert_eq!(
                Some(value),
                updated_checkout.read(correlation_id, &key).unwrap()
            );
        }

        let original_checkout = state.checkout(root_hash).unwrap().unwrap();
        for TestPair { key, value } in create_test_pairs().iter().cloned() {
            assert_eq!(
                Some(value),
                original_checkout.read(correlation_id, &key).unwrap()
            );
        }
        assert_eq!(
            None,
            original_checkout
                .read(correlation_id, &test_pairs_updated[2].key)
                .unwrap()
        );
    }

    #[test]
    fn copy_one_state_to_another() {
        let correlation_id = CorrelationId::new();
        let source_reader = {
            let (source_state, root_hash) = create_test_state();
            {
                // Make sure no missing nodes in source
                let missing_from_source = source_state
                    .missing_descendant_trie_keys(correlation_id, root_hash)
                    .unwrap();
                assert_eq!(missing_from_source, Vec::new());
            }
            source_state.checkout(root_hash).unwrap().unwrap()
        };

        let destination_state = new_empty_lmdb_global_state();

        // Copy source to destination
        let mut queue = vec![source_reader.root_hash];
        while !queue.is_empty() {
            let mut new_queue: Vec<Blake2bHash> = Vec::new();
            for trie_key in &queue {
                let trie_to_insert = source_reader
                    .read_trie(correlation_id, trie_key)
                    .unwrap()
                    .unwrap();
                destination_state
                    .put_trie(correlation_id, &trie_to_insert)
                    .unwrap();
                // Now that we've added in `trie_to_insert`, queue up its children
                let mut new_keys_to_enqueue = destination_state
                    .missing_descendant_trie_keys(correlation_id, *trie_key)
                    .unwrap();
                new_queue.append(&mut new_keys_to_enqueue);
            }
            queue = new_queue;
        }

        // After the copying process above there should be no missing entries in the destination
        {
            let missing_from_destination = destination_state
                .missing_descendant_trie_keys(correlation_id, source_reader.root_hash)
                .unwrap();

            assert_eq!(missing_from_destination, Vec::new());
        }

        // Make sure all of the destination keys under the root hash are in the source
        {
            let destination_keys = operations::keys::<Key, StoredValue, _, _>(
                correlation_id,
                &destination_state.environment.create_read_txn().unwrap(),
                destination_state.trie_store.deref(),
                &source_reader.root_hash,
            )
            .filter_map(Result::ok)
            .collect::<Vec<Key>>();
            for key in destination_keys {
                source_reader.read(correlation_id, &key).unwrap();
            }
        }

        // Make sure all of the source keys under the root hash are in the destination
        {
            let source_keys = operations::keys::<Key, StoredValue, _, _>(
                correlation_id,
                &source_reader.environment.create_read_txn().unwrap(),
                source_reader.store.deref(),
                &source_reader.root_hash,
            )
            .filter_map(Result::ok)
            .collect::<Vec<Key>>();
            let destination_reader = destination_state
                .checkout(source_reader.root_hash)
                .unwrap()
                .unwrap();
            for key in source_keys {
                destination_reader.read(correlation_id, &key).unwrap();
            }
        }
    }

    #[test]
    fn missing_descendant_trie_keys_should_catch_a_key_with_a_corrupt_value() {
        let correlation_id = CorrelationId::new();
        let source_reader = {
            let (source_state, root_hash) = create_test_state();
            source_state.checkout(root_hash).unwrap().unwrap()
        };

        let destination_state = new_empty_lmdb_global_state();

        // Copy source to destination
        // After processing 3 entries, put a corrupt entry in
        let mut queue = vec![source_reader.root_hash];
        let mut n = 3;
        while !queue.is_empty() {
            let mut new_queue: Vec<Blake2bHash> = Vec::new();

            for trie_key in &queue {
                n = n - 1;
                if n == 0 {
                    let bad_trie_value: Trie<Key, StoredValue> = Trie::Node {
                        pointer_block: Box::new(Default::default()),
                    };
                    let mut txn = destination_state
                        .environment
                        .create_read_write_txn()
                        .unwrap();
                    destination_state
                        .trie_store
                        .put(&mut txn, &trie_key, &bad_trie_value)
                        .unwrap();
                    txn.commit().unwrap();
                } else {
                    let trie_to_insert = source_reader
                        .read_trie(correlation_id, trie_key)
                        .unwrap()
                        .unwrap();
                    destination_state
                        .put_trie(correlation_id, &trie_to_insert)
                        .unwrap();
                    // Now that we've added in `trie_to_insert`, queue up its children
                    let mut new_keys_to_enqueue = destination_state
                        .missing_descendant_trie_keys(correlation_id, *trie_key)
                        .unwrap();
                    new_queue.append(&mut new_keys_to_enqueue);
                }
            }
            queue = new_queue;
        }

        // We've copied over all of the source to the destination, except for one `Trie<K,V>` and
        // its descendants.  When we look for missing descendants of the state root it should have
        // just one entry corresponding to the value that is corrupted.
        let missing_from_destination = destination_state
            .missing_descendant_trie_keys(correlation_id, source_reader.root_hash)
            .unwrap();

        let bad_key = match &*missing_from_destination {
            [bad_key] => bad_key,
            unexpected_missing_keys => {
                panic!("unexpected_missing_keys {:?}", unexpected_missing_keys)
            }
        };

        let hash_of_bad_trie_value = {
            let bad_trie_value = destination_state
                .checkout(source_reader.root_hash)
                .unwrap()
                .unwrap()
                .read_trie(correlation_id, bad_key)
                .unwrap()
                .unwrap();
            let node_bytes = bad_trie_value.to_bytes().unwrap();
            Blake2bHash::new(&node_bytes)
        };

        assert_ne!(*bad_key, hash_of_bad_trie_value);
    }
}
