use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use derive_more::derive::From;
use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct BroadcastMessage(u64);

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct BroadcastBody {
    msg_id: MessageId,
    message: BroadcastMessage,
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
    messages: Box<[BroadcastMessage]>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct TopologyBody {
    msg_id: MessageId,
    topology: HashMap<NodeId, Box<[NodeId]>>,
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
struct RetryEntry {
    message: Message<BroadcastBody>,
    send_after: Instant,
    count: u8,
}

fn send_after(current: Instant, retry_count: u8) -> Instant {
    current + Duration::from_millis(100) * (retry_count.max(9) + 1) as u32
}

impl RetryEntry {
    fn new(message: Message<BroadcastBody>) -> Self {
        let count = 0;
        Self {
            message,
            send_after: send_after(Instant::now(), count),
            count,
        }
    }

    fn with_backoff(mut self) -> Self {
        self.count += 1;
        self.send_after = send_after(self.send_after, self.count);
        self
    }
}

#[derive(Default, Debug)]
struct RetryQueue {
    inner: VecDeque<RetryEntry>,
}

impl RetryQueue {
    fn front(&self) -> Option<&RetryEntry> {
        self.inner.front()
    }

    fn insert(&mut self, entry: RetryEntry) {
        match self
            .inner
            .binary_search_by(|e| e.send_after.cmp(&entry.send_after))
        {
            Ok(idx) | Err(idx) => self.inner.insert(idx, entry),
        }
    }

    fn remove(&mut self, predicate: impl FnMut(&RetryEntry) -> bool) {
        if let Some(idx) = self.inner.iter().position(predicate) {
            self.inner.remove(idx);
        }
    }

    fn pop_entries_needing_retry(&mut self) -> impl Iterator<Item = RetryEntry> + '_ {
        if let Some(last_idx) = self
            .inner
            .iter()
            .rposition(|entry| entry.send_after <= Instant::now())
        {
            self.inner.drain(..=last_idx)
        } else {
            self.inner.drain(..0)
        }
    }
}

#[derive(Debug)]
struct BroadcastNode {
    id: NodeId,
    topology: HashMap<NodeId, Box<[NodeId]>>,
    msg_ids: MessageIdIter,
    messages: Vec<BroadcastMessage>,
    next_hop: Option<NodeId>,
    retry_queue: RetryQueue,
}

impl BroadcastNode {
    fn response<B>(&self, dest: NodeId, body: B) -> Message<B> {
        Message {
            src: self.id,
            dest,
            body,
        }
    }

    fn handle_broadcast(&mut self, src: NodeId, body: BroadcastBody) -> Vec<Message<MessageBody>> {
        let mut responses = vec![self.response(
            src,
            MessageBody::BroadcastOk(BroadcastOkBody {
                in_reply_to: body.msg_id,
            }),
        )];

        if !self.messages.contains(&body.message) {
            self.messages.push(body.message.clone());
            if let Some(next_hop) = self.next_hop {
                let msg_id = self
                    .msg_ids
                    .next()
                    .expect("exhausted available message ids");
                let body = BroadcastBody {
                    msg_id,
                    message: body.message,
                };
                self.retry_queue
                    .insert(RetryEntry::new(self.response(next_hop, body.clone())));
                responses.push(self.response(next_hop, MessageBody::Broadcast(body)));
            }
        }

        responses
    }

    fn handle_read(&mut self, src: NodeId, body: ReadBody) -> Vec<Message<MessageBody>> {
        vec![self.response(
            src,
            MessageBody::ReadOk(ReadOkBody {
                in_reply_to: body.msg_id,
                messages: self.messages.clone().into_iter().collect(),
            }),
        )]
    }

    fn handle_topology(&mut self, src: NodeId, body: TopologyBody) -> Vec<Message<MessageBody>> {
        self.topology = body.topology;
        vec![self.response(
            src,
            MessageBody::TopologyOk(TopologyOkBody {
                in_reply_to: body.msg_id,
            }),
        )]
    }

    fn handle_broadcast_ok(&mut self, body: BroadcastOkBody) -> Vec<Message<MessageBody>> {
        self.retry_queue
            .remove(|entry| entry.message.body.msg_id == body.in_reply_to);
        vec![]
    }
}

fn next_hop(id: NodeId, mut all_nodes: Box<[NodeId]>) -> Option<NodeId> {
    if all_nodes.len() < 2 {
        None
    } else {
        all_nodes.sort();
        if let Some(pos) = all_nodes.iter().position(|&x| x == id) {
            Some(all_nodes.get(pos + 1).copied().unwrap_or(all_nodes[0]))
        } else {
            panic!("Node id not found in all_nodes")
        }
    }
}

impl InitializedNode for BroadcastNode {
    type RequestBody = MessageBody;
    type ResponseBody = MessageBody;

    fn new(id: NodeId, all_nodes: Box<[NodeId]>) -> Self {
        Self {
            id,
            topology: HashMap::default(),
            msg_ids: MessageIdIter::default(),
            messages: Vec::new(),
            next_hop: next_hop(id, all_nodes),
            retry_queue: RetryQueue::default(),
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
        self.retry_queue.front().map(|entry| entry.send_after)
    }

    fn wake_up(&mut self) -> Vec<Message<MessageBody>> {
        let entries: Vec<_> = self.retry_queue.pop_entries_needing_retry().collect();
        let mut messages = vec![];
        for entry in entries {
            messages.push(entry.message.clone().mapped());
            self.retry_queue.insert(entry.with_backoff());
        }
        messages
    }
}

fn main() -> anyhow::Result<()> {
    run_node::<BroadcastNode>()
}
