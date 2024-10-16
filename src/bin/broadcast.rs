use std::{
    collections::{BTreeSet, HashMap},
    env,
    ops::AddAssign,
    time::{Duration, Instant},
};

use anyhow::Result;
use derive_more::derive::From;
use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, OneOrMany};

use outbox::Outbox;
use retry_queue::RetryQueue;

/// The type of values we are receiving and broadcast to other nodes.
type Value = u64;

#[serde_as]
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct BroadcastPayload {
    #[serde(rename = "message")]
    #[serde_as(as = "OneOrMany<_>")]
    values: Vec<Value>,
}

// We use `+=` to merge multiple broadcast messages into one for batching.
impl AddAssign<BroadcastPayload> for BroadcastPayload {
    fn add_assign(&mut self, rhs: BroadcastPayload) {
        for v in rhs.values {
            if !self.values.contains(&v) {
                self.values.push(v);
            }
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct ReadOkPayload {
    #[serde(rename = "messages")]
    values: Box<[Value]>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct TopologyPayload {
    topology: HashMap<NodeId, Vec<NodeId>>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, From)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Payload {
    Broadcast(BroadcastPayload),
    BroadcastOk,
    Read,
    ReadOk(ReadOkPayload),
    Topology(TopologyPayload),
    TopologyOk,
}

#[derive(Debug)]
struct BroadcastNode {
    id: NodeId,
    other_nodes: Box<[NodeId]>,
    tx: MessageTransmitter<Payload>,
    values: BTreeSet<Value>,
    outbox: Outbox<BroadcastPayload>,
    retry_queue: RetryQueue<BroadcastPayload>,
}

impl BroadcastNode {
    fn new(
        id: NodeId,
        all_nodes: &[NodeId],
        tx: MessageTransmitter<Payload>,
        broadcast_delay: Duration,
    ) -> Self {
        let other_nodes = all_nodes.iter().filter(|&&n| n != id).copied().collect();
        Self {
            id,
            other_nodes,
            tx,
            values: BTreeSet::default(),
            outbox: Outbox::new(broadcast_delay),
            retry_queue: RetryQueue::new(Duration::from_millis(250)),
        }
    }

    fn broadcast_destinations(&self, src: NodeId) -> Box<[NodeId]> {
        if self.id == src || self.other_nodes.contains(&src) {
            Box::new([])
        } else {
            self.other_nodes.clone()
        }
    }

    fn handle_broadcast(&mut self, Message { header, payload }: Message<BroadcastPayload>) {
        let values: BTreeSet<Value> = payload.values.into_iter().collect();
        let new_values: BTreeSet<Value> = values.difference(&self.values).copied().collect();
        self.values.extend(&new_values);

        let new_values: Vec<_> = new_values.into_iter().collect();
        if !new_values.is_empty() {
            for neighbor in self.broadcast_destinations(header.src) {
                self.outbox.merge_or_push(self.tx.prepare(
                    neighbor,
                    None,
                    BroadcastPayload {
                        values: new_values.clone(),
                    },
                ));
            }
        }

        self.tx.reply(&header, Payload::BroadcastOk);
    }

    fn handle_read(&mut self, header: &MessageHeader) {
        self.tx.reply(
            header,
            Payload::ReadOk(ReadOkPayload {
                values: self.values.clone().into_iter().collect(),
            }),
        );
    }

    fn handle_topology(&mut self, header: &MessageHeader) {
        self.tx.reply(header, Payload::TopologyOk);
    }

    fn handle_broadcast_ok(&mut self, header: &MessageHeader) {
        assert!(header.in_reply_to.is_some());
        self.retry_queue
            .remove(|message| message.header.msg_id == header.in_reply_to);
    }
}

impl NodeState for BroadcastNode {
    fn handle(mut self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        use Payload::*;
        let Message { header, payload } = deserialize_message(request)?;
        match payload {
            Broadcast(payload) => self.handle_broadcast(Message { header, payload }),
            Read => self.handle_read(&header),
            Topology(_) => self.handle_topology(&header),
            BroadcastOk => self.handle_broadcast_ok(&header),
            _ => (),
        }
        Ok(self)
    }

    fn next_wake_up(&self) -> Option<Instant> {
        match (self.outbox.send_after(), self.retry_queue.send_after()) {
            (None, None) => None,
            (None, Some(a)) | (Some(a), None) => Some(a),
            (Some(a), Some(b)) => Some(a.min(b)),
        }
    }

    fn wake_up(mut self: Box<Self>) -> Result<Box<dyn NodeState>> {
        for message in self.outbox.pop_messages_need_sending() {
            self.tx.send_message(&message.clone().mapped());
            self.retry_queue.insert(message);
        }
        for message in self
            .retry_queue
            .retry_messages()
            .into_iter()
            .map(Message::mapped)
        {
            self.tx.send_message(&message);
        }

        Ok(self)
    }
}

mod outbox {
    use std::{
        collections::VecDeque,
        ops::AddAssign,
        time::{Duration, Instant},
    };

    use super::Message;

    #[derive(Default, Debug)]
    pub struct Outbox<P> {
        inner: VecDeque<OutboxEntry<P>>,
        delay: Duration,
    }

    #[derive(Debug)]
    struct OutboxEntry<P> {
        message: Message<P>,
        send_after: Instant,
    }

    impl<P: AddAssign<P>> Outbox<P> {
        pub fn merge_or_push(&mut self, message: Message<P>) {
            if let Some(existing_entry) = self
                .inner
                .iter_mut()
                .find(|e| e.message.header.dest == message.header.dest)
            {
                existing_entry.message.payload += message.payload;
            } else {
                self.inner.push_back(OutboxEntry {
                    message,
                    send_after: Instant::now() + self.delay,
                });
            }
        }
    }

    impl<P> Outbox<P> {
        pub fn new(delay: Duration) -> Self {
            Self {
                inner: VecDeque::default(),
                delay,
            }
        }

        pub fn send_after(&self) -> Option<Instant> {
            self.inner.front().map(|entry| entry.send_after)
        }

        pub fn pop_messages_need_sending(&mut self) -> Vec<Message<P>> {
            if let Some(last_idx) = self
                .inner
                .iter()
                .rposition(|entry| entry.send_after <= Instant::now())
            {
                self.inner
                    .drain(..=last_idx)
                    .map(|entry| entry.message)
                    .collect()
            } else {
                vec![]
            }
        }
    }
}

mod retry_queue {
    use std::{
        collections::VecDeque,
        time::{Duration, Instant},
    };

    use super::Message;

    #[derive(Default, Debug)]
    pub struct RetryQueue<P> {
        inner: VecDeque<RetryEntry<P>>,
        backoff: Duration,
    }

    #[derive(Debug)]
    struct RetryEntry<P> {
        message: Message<P>,
        send_after: Instant,
        count: u8,
    }

    impl<P: Clone> RetryQueue<P> {
        pub fn new(backoff: Duration) -> Self {
            Self {
                inner: VecDeque::default(),
                backoff,
            }
        }

        fn backoff(&self, retry_count: u8) -> Instant {
            Instant::now() + self.backoff * u8::min(retry_count + 1, 5) as u32
        }

        pub fn send_after(&self) -> Option<Instant> {
            self.inner.front().map(|entry| entry.send_after)
        }

        fn insert_entry(&mut self, entry: RetryEntry<P>) {
            match self
                .inner
                .binary_search_by(|e| e.send_after.cmp(&entry.send_after))
            {
                Ok(idx) | Err(idx) => self.inner.insert(idx, entry),
            }
        }

        pub fn insert(&mut self, message: Message<P>) {
            self.insert_entry(RetryEntry {
                message,
                send_after: self.backoff(0),
                count: 0,
            });
        }

        pub fn remove(&mut self, mut predicate: impl FnMut(&Message<P>) -> bool) {
            if let Some(idx) = self
                .inner
                .iter()
                .position(|entry| predicate(&entry.message))
            {
                self.inner.remove(idx);
            }
        }

        pub fn retry_messages(&mut self) -> Vec<Message<P>> {
            if let Some(last_idx) = self
                .inner
                .iter()
                .rposition(|entry| entry.send_after <= Instant::now())
            {
                let entries: Vec<_> = self.inner.drain(..=last_idx).collect();
                let mut messages = Vec::new();
                for mut entry in entries {
                    messages.push(entry.message.clone());
                    entry.count += 1;
                    entry.send_after = self.backoff(entry.count);
                    self.insert_entry(entry);
                }
                messages
            } else {
                vec![]
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    let broadcast_delay = Duration::from_millis(
        env::var("BROADCAST_DELAY_MS")
            .unwrap_or("0".to_owned())
            .parse()?,
    );
    run_node(Box::new(move |init, tx| {
        Box::new(BroadcastNode::new(
            init.node_id,
            &init.node_ids,
            tx.into(),
            broadcast_delay,
        ))
    }))
}
