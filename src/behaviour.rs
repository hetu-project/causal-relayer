use libp2p::{gossipsub, identity, mdns, Multiaddr, PeerId};
use libp2p_swarm_derive::NetworkBehaviour;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::Duration;

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "NodeEvent")]
pub struct NodeBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

impl NodeBehaviour {
    pub fn new(id_keys: &identity::Keypair, peer_id: PeerId) -> Self {
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .validation_mode(gossipsub::ValidationMode::Permissive)
            .message_id_fn(|message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                gossipsub::MessageId::from(s.finish().to_string())
            })
            .build()
            .expect("Valid config");

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(id_keys.clone()),
            gossipsub_config,
        )
        .expect("Valid configuration");

        let mdns =
            mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id).expect("Valid config");

        NodeBehaviour { gossipsub, mdns }
    }
}

pub enum NodeEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
    PeerDiscovered(PeerId, Vec<Multiaddr>),
}

impl From<gossipsub::Event> for NodeEvent {
    fn from(event: gossipsub::Event) -> Self {
        NodeEvent::Gossipsub(event)
    }
}

impl From<mdns::Event> for NodeEvent {
    fn from(event: mdns::Event) -> Self {
        match event {
            mdns::Event::Discovered(list) => list
                .into_iter()
                .next()
                .map(|(peer_id, addrs)| NodeEvent::PeerDiscovered(peer_id, vec![addrs]))
                .unwrap_or(NodeEvent::Mdns(mdns::Event::Discovered(Vec::new()))),
            other => NodeEvent::Mdns(other),
        }
    }
}
