mod event;

use super::{fetcher::FetchResult, storage::Storage, Component};
use crate::{
    components::consensus::EraId,
    effect::{self, EffectBuilder, EffectExt, EffectOptionExt, Effects},
    types::{Block, BlockHash, FinalizedBlock},
};
use effect::requests::{
    BlockExecutorRequest, BlockValidationRequest, FetcherRequest, StorageRequest,
};
pub use event::Event;
use rand::{CryptoRng, Rng};
use std::fmt::Display;
use tracing::{error, info, trace, warn};

pub trait ReactorEventT<I>:
    From<StorageRequest<Storage>>
    + From<FetcherRequest<I, Block>>
    + From<BlockValidationRequest<Block, I>>
    + From<BlockExecutorRequest>
    + Send
{
}

impl<I, REv> ReactorEventT<I> for REv where
    REv: From<StorageRequest<Storage>>
        + From<FetcherRequest<I, Block>>
        + From<BlockValidationRequest<Block, I>>
        + From<BlockExecutorRequest>
        + Send
{
}

#[derive(Debug)]
pub(crate) struct LinearChainSync<I> {
    // Set of peers that we can requests block from.
    peers: Vec<I>,
    // Peers we have not yet requested current block from.
    // NOTE: Maybe use a bitmask to decide which peers were tried?.
    peers_to_try: Vec<I>,
    // Chain of downloaded blocks from the linear chain.
    linear_chain: Vec<Block>,
    // How many blocks of the linear chain we've synchronized.
    linear_chain_length: u64,
    // Flag indicating whether we have finished syncing linear chain.
    is_synced: bool,
    // Linear chain block to start sync from.
    init_hash: Option<BlockHash>,
    // TODO: remove when proper syncing is implemented
    // The era of the linear chain block to start sync from
    init_block_era: Option<EraId>,
    // During synchronization we might see new eras being created.
    // Track the highest height and wait until it's handled by consensus.
    highest_block_seen: u64,
}

impl<I: Clone + 'static> LinearChainSync<I> {
    #[allow(unused)]
    pub fn new<REv: ReactorEventT<I>>(
        effect_builder: EffectBuilder<REv>,
        init_hash: Option<BlockHash>,
    ) -> Self {
        LinearChainSync {
            peers: Vec::new(),
            peers_to_try: Vec::new(),
            linear_chain: Vec::new(),
            linear_chain_length: 0,
            is_synced: init_hash.is_none(),
            init_hash,
            init_block_era: None,
            highest_block_seen: 0,
        }
    }

    fn reset_peers(&mut self) {
        self.peers_to_try = self.peers.clone();
    }

    fn random_peer<R: Rng + ?Sized>(&mut self, rand: &mut R) -> Option<I> {
        let peers_count = self.peers_to_try.len();
        if peers_count == 0 {
            return None;
        }
        if peers_count == 1 {
            return Some(self.peers_to_try.pop().expect("Not to fail"));
        }
        let idx = rand.gen_range(0, peers_count);
        Some(self.peers_to_try.remove(idx))
    }

    // Unsafe version of `random_peer`.
    // Panics if no peer is available for querying.
    fn random_peer_unsafe<R: Rng + ?Sized>(&mut self, rand: &mut R) -> I {
        self.random_peer(rand)
            .expect("At least one peer available.")
    }

    fn new_block(&mut self, block: Block) {
        self.linear_chain.push(block);
        self.linear_chain_length += 1;
    }

    /// Returns `true` if we have finished syncing linear chain.
    pub fn is_synced(&self) -> bool {
        self.is_synced
    }

    fn fetch_next_block_deploys<R, REv>(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        rng: &mut R,
    ) -> Effects<Event<I>>
    where
        I: Send + Copy + 'static,
        R: Rng + CryptoRng + ?Sized,
        REv: ReactorEventT<I>,
    {
        let peer = self.random_peer_unsafe(rng);
        match self.linear_chain.pop() {
            None => {
                // We're done syncing but we have to wait for the execution of all blocks.
                Effects::new()
            }
            Some(block) => fetch_block_deploys(effect_builder, peer, block),
        }
    }

    pub(crate) fn init_block_era(&self) -> Option<EraId> {
        self.init_block_era
    }
}

impl<I, REv, R> Component<REv, R> for LinearChainSync<I>
where
    I: Display + Clone + Copy + Send + 'static,
    R: Rng + CryptoRng + ?Sized,
    REv: ReactorEventT<I>,
{
    type Event = Event<I>;

    fn handle_event(
        &mut self,
        effect_builder: EffectBuilder<REv>,
        rng: &mut R,
        event: Self::Event,
    ) -> Effects<Self::Event> {
        match event {
            Event::Start(init_peer) => {
                match self.init_hash {
                    None => {
                        // No syncing configured.
                        Effects::new()
                    }
                    Some(init_hash) => {
                        trace!(?init_hash, "Start synchronization");
                        // Start synchronization.
                        fetch_block(effect_builder, init_peer, init_hash)
                    }
                }
            }
            Event::BlockExecutionDone(block_hash, block_height) => {
                info!(
                    ?block_hash,
                    ?block_height,
                    "Finished linear chain blocks execution."
                );
                Effects::new()
            }
            Event::GetBlockResult(block_hash, fetch_result) => match fetch_result {
                None => match self.random_peer(rng) {
                    None => {
                        error!(%block_hash, "Could not download linear block from any of the peers.");
                        panic!("Failed to download linear chain.")
                    }
                    Some(peer) => fetch_block(effect_builder, peer, block_hash),
                },
                Some(FetchResult::FromStorage(block)) => {
                    // remember the era of the init block
                    if Some(*block.hash()) == self.init_hash {
                        self.init_block_era = Some(block.era_id());
                    }
                    // We should be checking the local storage for linear blocks before we start
                    // syncing.
                    trace!(%block_hash, "Linear block found in the local storage.");
                    // If we found the linear block in the storage it means we should have all of
                    // its parents as well. If that's not the case then we have a bug.
                    effect_builder
                        .immediately()
                        .event(move |_| Event::LinearChainBlocksDownloaded)
                }
                Some(FetchResult::FromPeer(block, peer)) => {
                    // remember the era of the init block
                    if Some(*block.hash()) == self.init_hash {
                        self.init_block_era = Some(block.era_id());
                    }
                    if *block.hash() != block_hash {
                        warn!(
                            "Block hash mismatch. Expected {} got {} from {}.",
                            block_hash,
                            block.hash(),
                            peer
                        );
                        // NOTE: Signal misbehaving validator to networking layer.
                        return self.handle_event(
                            effect_builder,
                            rng,
                            Event::GetBlockResult(block_hash, None),
                        );
                    }
                    trace!(%block_hash, "Downloaded linear chain block.");
                    self.reset_peers();
                    self.new_block(*block.clone());
                    let curr_height = block.height();
                    // We instantiate with `highest_block_seen=0`, start downloading with the
                    // highest block and then download its ancestors. It should
                    // be updated only once at the start.
                    if curr_height > self.highest_block_seen {
                        self.highest_block_seen = curr_height;
                    }
                    if block.is_genesis_child() {
                        info!("Linear chain downloaded. Starting downloading deploys.");
                        effect_builder
                            .immediately()
                            .event(move |_| Event::LinearChainBlocksDownloaded)
                    } else {
                        let parent_hash = *block.parent_hash();
                        let peer = self.random_peer_unsafe(rng);
                        fetch_block(effect_builder, peer, parent_hash)
                    }
                }
            },
            Event::DeploysFound(block) => {
                let block_hash = *block.hash();
                let block_height = block.height();
                trace!(%block_hash, "Deploys for linear chain block found.");
                // Reset used peers so we can download next block with the full set.
                self.reset_peers();
                // Execute block
                // Download next block deploys.
                let mut effects = self.fetch_next_block_deploys(effect_builder, rng);
                let finalized_block: FinalizedBlock = (*block).into();
                let execute_block_effect = effect_builder
                    .execute_block(finalized_block)
                    .event(move |_| Event::BlockExecutionDone(block_hash, block_height));
                effects.extend(execute_block_effect);
                effects
            }
            Event::DeploysNotFound(block) => match self.random_peer(rng) {
                None => {
                    let block_hash = block.hash();
                    error!(%block_hash, "Could not download deploys from linear chain block.");
                    panic!("Failed to download linear chain deploys.")
                }
                Some(peer) => fetch_block_deploys(effect_builder, peer, *block),
            },
            Event::LinearChainBlocksDownloaded => {
                // Start downloading deploys from the first block of the linear chain.
                self.fetch_next_block_deploys(effect_builder, rng)
            }
            Event::NewPeerConnected(peer_id) => {
                trace!(%peer_id, "New peer connected");
                let mut effects = Effects::new();
                if self.peers.is_empty() {
                    // First peer connected, start dowloading.
                    effects.extend(
                        effect_builder
                            .immediately()
                            .event(move |_| Event::Start(peer_id)),
                    );
                }
                // Add to the set of peers we can request things from.
                self.peers.push(peer_id);
                effects
            }
            Event::BlockHandled(height) => {
                if height == self.highest_block_seen {
                    info!(%height, "Finished synchronizing linear chain.");
                    self.is_synced = true;
                }
                Effects::new()
            }
        }
    }
}

fn fetch_block_deploys<I: Send + Copy + 'static, REv>(
    effect_builder: EffectBuilder<REv>,
    peer: I,
    block: Block,
) -> Effects<Event<I>>
where
    REv: ReactorEventT<I>,
{
    effect_builder
        .validate_block(peer, block)
        .event(move |(found, block)| {
            if found {
                Event::DeploysFound(Box::new(block))
            } else {
                Event::DeploysNotFound(Box::new(block))
            }
        })
}

fn fetch_block<I: Send + Copy + 'static, REv>(
    effect_builder: EffectBuilder<REv>,
    peer: I,
    block_hash: BlockHash,
) -> Effects<Event<I>>
where
    REv: ReactorEventT<I>,
{
    effect_builder.fetch_block(block_hash, peer).option(
        move |value| Event::GetBlockResult(block_hash, Some(value)),
        move || Event::GetBlockResult(block_hash, None),
    )
}
