//! Deploy buffer.
//!
//! The deploy buffer stores deploy hashes in memory, tracking their suitability for inclusion into
//! a new block. Upon request, it returns a list of candidates that can be included.

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Display, Formatter},
    time::Duration,
};

use datasize::DataSize;
use derive_more::From;
use fmt::Debug;
use rand::{CryptoRng, Rng};
use semver::Version;
use tracing::{error, info};

use crate::{
    components::{chainspec_loader::DeployConfig, storage::Storage, Component},
    effect::{
        requests::{DeployBufferRequest, StorageRequest},
        EffectBuilder, EffectExt, Effects, Responder,
    },
    reactor::EventQueueHandle,
    types::{DeployHash, DeployHeader, ProtoBlock, ProtoBlockHash, Timestamp},
    Chainspec,
};

const DEPLOY_BUFFER_PRUNE_INTERVAL: Duration = Duration::from_secs(10);

/// An event for when using the deploy buffer as a component.
#[derive(Debug, From)]
pub enum Event {
    #[from]
    Request(DeployBufferRequest),
    /// A new deploy should be buffered.
    Buffer {
        hash: DeployHash,
        header: Box<DeployHeader>,
    },
    /// The deploy-buffer has been asked to prune stale deploys
    BufferPrune,
    /// A proto block has been proposed. We should not propose duplicates of its deploys.
    ProposedProtoBlock(ProtoBlock),
    /// A proto block has been finalized. We should never propose its deploys again.
    FinalizedProtoBlock(ProtoBlock),
    /// A proto block has been orphaned. Its deploys should be re-proposed.
    OrphanedProtoBlock(ProtoBlock),
    /// The result of the `DeployBuffer` getting the chainspec from the storage component.
    GetChainspecResult {
        maybe_chainspec: Box<Option<Chainspec>>,
        current_instant: Timestamp,
        past_blocks: HashSet<ProtoBlockHash>,
        responder: Responder<HashSet<DeployHash>>,
    },
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Event::BufferPrune => write!(f, "buffer prune"),
            Event::Request(req) => write!(f, "deploy-buffer request: {}", req),
            Event::Buffer { hash, .. } => write!(f, "deploy-buffer add {}", hash),
            Event::ProposedProtoBlock(block) => {
                write!(f, "deploy-buffer proposed proto block {}", block)
            }
            Event::FinalizedProtoBlock(block) => {
                write!(f, "deploy-buffer finalized proto block {}", block)
            }
            Event::OrphanedProtoBlock(block) => {
                write!(f, "deploy-buffer orphaned proto block {}", block)
            }
            Event::GetChainspecResult {
                maybe_chainspec, ..
            } => {
                if maybe_chainspec.is_some() {
                    write!(f, "deploy-buffer got chainspec")
                } else {
                    write!(f, "deploy-buffer failed to get chainspec")
                }
            }
        }
    }
}

type DeployCollection = HashMap<DeployHash, DeployHeader>;
type ProtoBlockCollection = HashMap<ProtoBlockHash, DeployCollection>;

pub(crate) trait ReactorEventT:
    From<Event> + From<StorageRequest<Storage>> + Send + 'static
{
}

impl<REv> ReactorEventT for REv where
    REv: From<Event> + From<StorageRequest<Storage>> + Send + 'static
{
}

/// Deploy buffer.
#[derive(DataSize, Debug, Clone)]
pub(crate) struct DeployBuffer {
    block_max_deploy_count: usize,
    pending: DeployCollection,
    proposed: ProtoBlockCollection,
    finalized: ProtoBlockCollection,
}

impl DeployBuffer {
    /// Creates a new, empty deploy buffer instance.
    pub(crate) fn new<REv>(
        event_queue: EventQueueHandle<REv>,
        block_max_deploy_count: usize,
    ) -> (Self, Effects<Event>)
    where
        REv: ReactorEventT,
    {
        let this = DeployBuffer {
            block_max_deploy_count,
            pending: HashMap::new(),
            proposed: HashMap::new(),
            finalized: HashMap::new(),
        };
        let effect_builder = EffectBuilder::new(event_queue);
        let cleanup = effect_builder
            .set_timeout(DEPLOY_BUFFER_PRUNE_INTERVAL)
            .event(|_| Event::BufferPrune);
        (this, cleanup)
    }

    /// Adds a deploy to the deploy buffer.
    ///
    /// Returns `false` if the deploy has been rejected.
    fn add_deploy(&mut self, hash: DeployHash, header: DeployHeader) {
        // only add the deploy if it isn't contained in a finalized block
        if !self
            .finalized
            .values()
            .any(|block| block.contains_key(&hash))
        {
            self.pending.insert(hash, header);
            info!("added deploy {} to the buffer", hash);
        } else {
            info!("deploy {} rejected from the buffer", hash);
        }
    }

    /// Gets the chainspec from storage in order to call `remaining_deploys()`.
    fn get_chainspec_from_storage<REv: ReactorEventT>(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        current_instant: Timestamp,
        past_blocks: HashSet<ProtoBlockHash>,
        responder: Responder<HashSet<DeployHash>>,
    ) -> Effects<Event> {
        // TODO - should the current protocol version be passed in here?
        let version = Version::from((1, 0, 0));
        effect_builder
            .get_chainspec(version)
            .event(move |maybe_chainspec| Event::GetChainspecResult {
                maybe_chainspec: Box::new(maybe_chainspec),
                current_instant,
                past_blocks,
                responder,
            })
    }

    /// Returns a list of candidates for inclusion into a block.
    fn remaining_deploys(
        &mut self,
        deploy_config: DeployConfig,
        current_instant: Timestamp,
        past_blocks: HashSet<ProtoBlockHash>,
    ) -> HashSet<DeployHash> {
        let past_deploys = past_blocks
            .iter()
            .filter_map(|block_hash| self.proposed.get(block_hash))
            .chain(self.finalized.values())
            .flat_map(|deploys| deploys.keys())
            .collect::<HashSet<_>>();
        // deploys_to_return = all deploys in pending that aren't in finalized blocks or
        // proposed blocks from the set `past_blocks`
        self.pending
            .iter()
            .filter(|&(hash, deploy)| {
                self.is_deploy_valid(deploy, current_instant, &deploy_config, &past_deploys)
                    && !past_deploys.contains(hash)
            })
            .map(|(hash, _deploy)| *hash)
            .take(self.block_max_deploy_count)
            .collect::<HashSet<_>>()
        // TODO: check gas and block size limits
    }

    /// Checks if a deploy is valid (for inclusion into the next block).
    fn is_deploy_valid(
        &self,
        deploy: &DeployHeader,
        current_instant: Timestamp,
        deploy_config: &DeployConfig,
        past_deploys: &HashSet<&DeployHash>,
    ) -> bool {
        let all_deps_resolved = || {
            deploy
                .dependencies()
                .iter()
                .all(|dep| past_deploys.contains(dep))
        };
        let ttl_valid = deploy.ttl() <= deploy_config.max_ttl;
        let timestamp_valid = deploy.timestamp() <= current_instant;
        let deploy_valid = deploy.timestamp() + deploy.ttl() >= current_instant;
        let num_deps_valid = deploy.dependencies().len() <= deploy_config.max_dependencies as usize;
        ttl_valid && timestamp_valid && deploy_valid && num_deps_valid && all_deps_resolved()
    }

    /// Notifies the deploy buffer of a new block that has been proposed, so that the block's
    /// deploys are not returned again by `remaining_deploys`.
    fn added_block<I>(&mut self, block: ProtoBlockHash, deploys: I)
    where
        I: IntoIterator<Item = DeployHash>,
    {
        // TODO: This will ignore deploys that weren't in `pending`. They might be added
        // later, and then would be proposed as duplicates.
        let deploy_map: HashMap<_, _> = deploys
            .into_iter()
            .filter_map(|deploy_hash| {
                self.pending
                    .get(&deploy_hash)
                    .map(|deploy| (deploy_hash, deploy.clone()))
            })
            .collect();
        self.pending
            .retain(|deploy_hash, _| !deploy_map.contains_key(deploy_hash));
        self.proposed.insert(block, deploy_map);
    }

    /// Notifies the deploy buffer that a block has been finalized.
    fn finalized_block(&mut self, block: ProtoBlockHash) {
        if let Some(deploys) = self.proposed.remove(&block) {
            self.pending
                .retain(|deploy_hash, _| !deploys.contains_key(deploy_hash));
            self.finalized.insert(block, deploys);
        } else if !block.is_empty() {
            // TODO: Events are not guaranteed to be handled in order, so this could happen!
            error!("finalized block that hasn't been proposed!");
        }
    }

    /// Notifies the deploy buffer that a block has been orphaned.
    fn orphaned_block(&mut self, block: ProtoBlockHash) {
        if let Some(deploys) = self.proposed.remove(&block) {
            self.pending.extend(deploys);
        } else {
            // TODO: Events are not guaranteed to be handled in order, so this could happen!
            error!("orphaned block that hasn't been proposed!");
        }
    }

    /// Prunes stale deploy information from the DeployBuffer, returns the total deploys pruned
    fn prune(&mut self) -> usize {
        fn prune_collection(map: &mut DeployCollection) -> usize {
            let initial_len = map.len();
            map.retain(|_hash, header| {
                let now = Timestamp::now();
                let lifespan = header.timestamp() + header.ttl();
                lifespan > now
            });
            initial_len - map.len()
        }
        fn prune_proto_collection(proto_collection: &mut ProtoBlockCollection) -> usize {
            let mut pruned = 0;
            let mut remove = Vec::new();
            for (proto_hash, proposed) in proto_collection.iter_mut() {
                pruned += prune_collection(proposed);
                if proposed.is_empty() {
                    remove.push(*proto_hash);
                }
            }
            proto_collection.retain(|k, _v| !remove.contains(&k));
            pruned
        }
        let collected = prune_collection(&mut self.pending);
        let proposed = prune_proto_collection(&mut self.proposed);
        collected + proposed
    }
}

impl<REv, R> Component<REv, R> for DeployBuffer
where
    REv: ReactorEventT,
    R: Rng + CryptoRng + ?Sized,
{
    type Event = Event;

    fn handle_event(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        _rng: &mut R,
        event: Self::Event,
    ) -> Effects<Self::Event> {
        match event {
            Event::BufferPrune => {
                let pruned = self.prune();
                log::debug!("Pruned {} deploys from buffer", pruned);
                return effect_builder
                    .set_timeout(DEPLOY_BUFFER_PRUNE_INTERVAL)
                    .event(|_| Event::BufferPrune);
            }
            Event::Request(DeployBufferRequest::ListForInclusion {
                current_instant,
                past_blocks,
                responder,
            }) => {
                return self.get_chainspec_from_storage(
                    effect_builder,
                    current_instant,
                    past_blocks,
                    responder,
                );
            }
            Event::Buffer { hash, header } => self.add_deploy(hash, *header),
            Event::ProposedProtoBlock(block) => {
                let (hash, deploys, _) = block.destructure();
                self.added_block(hash, deploys)
            }
            Event::FinalizedProtoBlock(block) => self.finalized_block(*block.hash()),
            Event::OrphanedProtoBlock(block) => self.orphaned_block(*block.hash()),
            Event::GetChainspecResult {
                maybe_chainspec,
                current_instant,
                past_blocks,
                responder,
            } => {
                let chainspec = maybe_chainspec.expect("should return chainspec");
                let deploys = self.remaining_deploys(
                    chainspec.genesis.deploy_config,
                    current_instant,
                    past_blocks,
                );
                return responder.respond(deploys).ignore();
            }
        }
        Effects::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use casper_execution_engine::core::engine_state::executable_deploy_item::ExecutableDeployItem;
    use rand::random;

    use super::*;
    use crate::{
        crypto::{asymmetric_key::SecretKey, hash::hash},
        reactor::{EventQueueHandle, QueueKind, Scheduler},
        testing::TestRng,
        types::{Deploy, DeployHash, DeployHeader, NodeConfig, ProtoBlockHash, TimeDiff},
        utils,
    };

    fn generate_deploy(
        rng: &mut TestRng,
        timestamp: Timestamp,
        ttl: TimeDiff,
        dependencies: Vec<DeployHash>,
    ) -> (DeployHash, DeployHeader) {
        let secret_key = SecretKey::random(rng);
        let gas_price = 10;
        let chain_name = "chain".to_string();
        let payment = ExecutableDeployItem::ModuleBytes {
            module_bytes: vec![],
            args: vec![],
        };
        let session = ExecutableDeployItem::ModuleBytes {
            module_bytes: vec![],
            args: vec![],
        };

        let deploy = Deploy::new(
            timestamp,
            ttl,
            gas_price,
            dependencies,
            chain_name,
            payment,
            session,
            &secret_key,
            rng,
        );

        (*deploy.id(), deploy.take_header())
    }

    fn create_test_buffer() -> (DeployBuffer, Effects<Event>) {
        let scheduler = utils::leak(Scheduler::<Event>::new(QueueKind::weights()));
        let event_queue = EventQueueHandle::new(&scheduler);
        let node_cfg = NodeConfig::default();
        DeployBuffer::new(event_queue, node_cfg.block_max_deploy_count as usize)
    }

    impl From<StorageRequest<Storage>> for Event {
        fn from(_: StorageRequest<Storage>) -> Self {
            // we never send a storage request in our unit tests, but if this does become
            // meaningful...
            todo!()
        }
    }

    #[test]
    fn add_and_take_deploys() {
        let creation_time = Timestamp::from(100);
        let ttl = TimeDiff::from(100);
        let block_time1 = Timestamp::from(80);
        let block_time2 = Timestamp::from(120);
        let block_time3 = Timestamp::from(220);

        let no_blocks = HashSet::new();
        let (mut buffer, _effects) = create_test_buffer();
        let mut rng = TestRng::new();
        let (hash1, deploy1) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash2, deploy2) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash3, deploy3) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash4, deploy4) = generate_deploy(&mut rng, creation_time, ttl, vec![]);

        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
            .is_empty());

        // add two deploys
        buffer.add_deploy(hash1, deploy1);
        buffer.add_deploy(hash2, deploy2.clone());

        // if we try to create a block with a timestamp that is too early, we shouldn't get any
        // deploys
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time1, no_blocks.clone())
            .is_empty());

        // if we try to create a block with a timestamp that is too late, we shouldn't get any
        // deploys, either
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time3, no_blocks.clone())
            .is_empty());

        // take the deploys out
        let deploys =
            buffer.remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone());

        assert_eq!(deploys.len(), 2);
        assert!(deploys.contains(&hash1));
        assert!(deploys.contains(&hash2));

        // the deploys should not have been removed yet
        assert!(!buffer
            .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
            .is_empty());

        // the two deploys will be included in block 1
        let block_hash1 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        buffer.added_block(block_hash1, deploys);

        // the deploys should have been removed now
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
            .is_empty());

        let mut blocks = HashSet::new();
        blocks.insert(block_hash1);

        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, blocks.clone())
            .is_empty());

        // try adding the same deploy again
        buffer.add_deploy(hash2, deploy2.clone());

        // it shouldn't be returned if we include block 1 in the past blocks
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time2, blocks)
            .is_empty());
        // ...but it should be returned if we don't include it
        assert!(
            buffer
                .remaining_deploys(DeployConfig::default(), block_time2, no_blocks.clone())
                .len()
                == 1
        );

        // the previous check removed the deploy from the buffer, let's re-add it
        buffer.add_deploy(hash2, deploy2);

        // finalize the block
        buffer.finalized_block(block_hash1);

        // add more deploys
        buffer.add_deploy(hash3, deploy3);
        buffer.add_deploy(hash4, deploy4);

        let deploys = buffer.remaining_deploys(DeployConfig::default(), block_time2, no_blocks);

        // since block 1 is now finalized, deploy2 shouldn't be among the ones returned
        assert_eq!(deploys.len(), 2);
        assert!(deploys.contains(&hash3));
        assert!(deploys.contains(&hash4));
    }

    #[test]
    fn test_prune() {
        let creation_time = Timestamp::from(100);
        let ttl = TimeDiff::from(100);

        let mut rng = TestRng::new();
        let (hash1, deploy1) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash2, deploy2) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash3, deploy3) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        let (hash4, deploy4) = generate_deploy(
            &mut rng,
            Timestamp::now() + Duration::from_secs(20).into(),
            ttl,
            vec![],
        );
        let (mut buffer, _effects) = create_test_buffer();

        // pending
        buffer.add_deploy(hash1, deploy1);
        buffer.add_deploy(hash2, deploy2);
        buffer.add_deploy(hash3, deploy3);
        buffer.add_deploy(hash4, deploy4);

        // pending => proposed
        let block_hash1 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        let block_hash2 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        buffer.added_block(block_hash1, vec![hash1]);
        buffer.added_block(block_hash2, vec![hash2]);

        // proposed => finalized
        buffer.finalized_block(block_hash1);

        assert_eq!(buffer.pending.len(), 2);
        assert_eq!(buffer.proposed.get(&block_hash2).unwrap().len(), 1);
        assert_eq!(buffer.finalized.get(&block_hash1).unwrap().len(), 1);

        let pruned = buffer.prune();
        assert_eq!(pruned, 2);

        // deploy with hash 3 was never processed, and passed ttl, so it should be gone
        assert_eq!(buffer.pending.len(), 1);

        // Proposed map should be -empty-, as the deploys under block_hash1 have all been removed
        assert_eq!(buffer.proposed.get(&block_hash2), None);

        // Finalized should NOT be changed - 1 block
        assert_eq!(buffer.finalized.len(), 1);

        // with one deploy in it
        assert_eq!(buffer.finalized.get(&block_hash1).unwrap().len(), 1);
    }

    #[test]
    fn test_deploy_dependencies() {
        let creation_time = Timestamp::from(100);
        let ttl = TimeDiff::from(100);
        let block_time = Timestamp::from(120);

        let mut rng = TestRng::new();
        let (hash1, deploy1) = generate_deploy(&mut rng, creation_time, ttl, vec![]);
        // let deploy2 depend on deploy1
        let (hash2, deploy2) = generate_deploy(&mut rng, creation_time, ttl, vec![hash1]);

        let mut blocks = HashSet::new();
        let (mut buffer, _effects) = create_test_buffer();

        // add deploy2
        buffer.add_deploy(hash2, deploy2);

        // deploy2 has an unsatisfied dependency
        assert!(buffer
            .remaining_deploys(DeployConfig::default(), block_time, blocks.clone())
            .is_empty());

        // add deploy1
        buffer.add_deploy(hash1, deploy1);

        let deploys = buffer.remaining_deploys(DeployConfig::default(), block_time, blocks.clone());
        // only deploy1 should be returned, as it has no dependencies
        assert_eq!(deploys.len(), 1);
        assert!(deploys.contains(&hash1));

        // the deploy will be included in block 1
        let block_hash1 = ProtoBlockHash::new(hash(random::<[u8; 16]>()));
        buffer.added_block(block_hash1, deploys);
        blocks.insert(block_hash1);

        let deploys2 = buffer.remaining_deploys(DeployConfig::default(), block_time, blocks);
        // `blocks` contains a block that contains deploy1 now, so we should get deploy2
        assert_eq!(deploys2.len(), 1);
        assert!(deploys2.contains(&hash2));
    }
}
