use std::{
    collections::{BTreeSet, HashMap},
    env,
    ops::AddAssign,
    time::{Duration, Instant},
};

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
struct BroadcastBody {
    msg_id: MessageId,
    #[serde(rename = "message")]
    #[serde_as(as = "OneOrMany<_>")]
    values: Vec<Value>,
}

// We use `+=` to merge multiple broadcast messages into one for batching.
impl AddAssign<BroadcastBody> for BroadcastBody {
    fn add_assign(&mut self, rhs: BroadcastBody) {
        for v in rhs.values {
            if !self.values.contains(&v) {
                self.values.push(v);
            }
        }
    }
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct BroadcastOkBody {
    in_reply_to: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct ReadBody {
    msg_id: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct ReadOkBody {
    in_reply_to: MessageId,
    #[serde(rename = "messages")]
    values: Box<[Value]>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct TopologyBody {
    msg_id: MessageId,
    topology: HashMap<NodeId, Vec<NodeId>>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct TopologyOkBody {
    in_reply_to: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, From)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageBody {
    Broadcast(BroadcastBody),
    BroadcastOk(BroadcastOkBody),
    Read(ReadBody),
    ReadOk(ReadOkBody),
    Topology(TopologyBody),
    TopologyOk(TopologyOkBody),
}

#[derive(Debug)]
struct BroadcastNode {
    id: NodeId,
    all_nodes: Vec<NodeId>,
    msg_ids: MessageIdGenerator,
    values: BTreeSet<Value>,
    outbox: Outbox<BroadcastBody>,
    retry_queue: RetryQueue<BroadcastBody>,
}

impl BroadcastNode {
    fn response<B>(&self, dest: NodeId, body: B) -> Message<B> {
        Message {
            src: self.id,
            dest,
            body,
        }
    }

    fn broadcast_neighbors(&self, src: NodeId) -> Vec<NodeId> {
        if self.all_nodes.contains(&src) {
            vec![]
        } else {
            self.all_nodes
                .iter()
                .filter(|&&n| n != self.id)
                .copied()
                .collect()
        }
    }

    fn handle_broadcast(&mut self, src: NodeId, body: BroadcastBody) -> Vec<Message<MessageBody>> {
        let values: BTreeSet<Value> = body.values.into_iter().collect();
        let new_values: BTreeSet<Value> = values.difference(&self.values).copied().collect();
        self.values.extend(&new_values);

        if !new_values.is_empty() {
            for neighbor in self.broadcast_neighbors(src) {
                let msg_id = self.msg_ids.next_id();
                self.outbox.merge_or_push(self.response(
                    neighbor,
                    BroadcastBody {
                        msg_id,
                        values: new_values.iter().copied().collect(),
                    },
                ));
            }
        }

        vec![self.response(
            src,
            MessageBody::BroadcastOk(BroadcastOkBody {
                in_reply_to: body.msg_id,
            }),
        )]
    }

    fn handle_read(&mut self, src: NodeId, body: ReadBody) -> Vec<Message<MessageBody>> {
        vec![self.response(
            src,
            MessageBody::ReadOk(ReadOkBody {
                in_reply_to: body.msg_id,
                values: self.values.clone().into_iter().collect(),
            }),
        )]
    }

    fn handle_topology(&mut self, src: NodeId, body: TopologyBody) -> Vec<Message<MessageBody>> {
        vec![self.response(
            src,
            MessageBody::TopologyOk(TopologyOkBody {
                in_reply_to: body.msg_id,
            }),
        )]
    }

    fn handle_broadcast_ok(&mut self, body: BroadcastOkBody) -> Vec<Message<MessageBody>> {
        self.retry_queue
            .remove(|message| message.body.msg_id == body.in_reply_to);
        vec![]
    }
}

impl InitializedNode for BroadcastNode {
    type RequestBody = MessageBody;
    type ResponseBody = MessageBody;

    fn new(id: NodeId, all_nodes: Box<[NodeId]>) -> Self {
        let broadcast_delay = Duration::from_millis(
            env::var("BROADCAST_DELAY_MS")
                .unwrap_or("0".to_owned())
                .parse()
                .expect("BROADCAST_DELAY_MS must be an integer"),
        );
        Self {
            id,
            all_nodes: all_nodes.to_vec(),
            msg_ids: MessageIdGenerator::default(),
            values: BTreeSet::default(),
            outbox: Outbox::new(broadcast_delay),
            retry_queue: RetryQueue::new(Duration::from_millis(250)),
        }
    }

    fn handle(&mut self, request: Message<MessageBody>) -> Vec<Message<MessageBody>> {
        use MessageBody::*;
        match request.body {
            Broadcast(body) => self.handle_broadcast(request.src, body),
            Read(body) => self.handle_read(request.src, body),
            Topology(body) => self.handle_topology(request.src, body),
            BroadcastOk(body) => self.handle_broadcast_ok(body),
            _ => vec![],
        }
    }

    fn next_wake_up(&self) -> Option<Instant> {
        match (self.outbox.send_after(), self.retry_queue.send_after()) {
            (None, None) => None,
            (None, Some(a)) | (Some(a), None) => Some(a),
            (Some(a), Some(b)) => Some(a.min(b)),
        }
    }

    fn wake_up(&mut self) -> Vec<Message<MessageBody>> {
        let mut messages = vec![];
        for message in self.outbox.pop_messages_need_sending() {
            messages.push(message.clone().mapped());
            self.retry_queue.insert(message);
        }
        messages.extend(
            self.retry_queue
                .retry_messages()
                .into_iter()
                .map(Message::mapped),
        );
        messages
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
    pub struct Outbox<B> {
        inner: VecDeque<OutboxEntry<B>>,
        delay: Duration,
    }

    #[derive(Debug)]
    struct OutboxEntry<B> {
        message: Message<B>,
        send_after: Instant,
    }

    impl<B: AddAssign<B>> Outbox<B> {
        pub fn merge_or_push(&mut self, message: Message<B>) {
            if let Some(existing_entry) = self
                .inner
                .iter_mut()
                .find(|e| e.message.dest == message.dest)
            {
                existing_entry.message.body += message.body;
            } else {
                self.inner.push_back(OutboxEntry {
                    message,
                    send_after: Instant::now() + self.delay,
                });
            }
        }
    }

    impl<B> Outbox<B> {
        pub fn new(delay: Duration) -> Self {
            Self {
                inner: VecDeque::default(),
                delay,
            }
        }

        pub fn send_after(&self) -> Option<Instant> {
            self.inner.front().map(|entry| entry.send_after)
        }

        pub fn pop_messages_need_sending(&mut self) -> Vec<Message<B>> {
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
    pub struct RetryQueue<B> {
        inner: VecDeque<RetryEntry<B>>,
        backoff: Duration,
    }

    #[derive(Debug)]
    struct RetryEntry<B> {
        message: Message<B>,
        send_after: Instant,
        count: u8,
    }

    impl<B: Clone> RetryQueue<B> {
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

        fn insert_entry(&mut self, entry: RetryEntry<B>) {
            match self
                .inner
                .binary_search_by(|e| e.send_after.cmp(&entry.send_after))
            {
                Ok(idx) | Err(idx) => self.inner.insert(idx, entry),
            }
        }

        pub fn insert(&mut self, message: Message<B>) {
            self.insert_entry(RetryEntry {
                message,
                send_after: self.backoff(0),
                count: 0,
            });
        }

        pub fn remove(&mut self, mut predicate: impl FnMut(&Message<B>) -> bool) {
            if let Some(idx) = self
                .inner
                .iter()
                .position(|entry| predicate(&entry.message))
            {
                self.inner.remove(idx);
            }
        }

        pub fn retry_messages(&mut self) -> Vec<Message<B>> {
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
    run_node::<BroadcastNode>()
}
