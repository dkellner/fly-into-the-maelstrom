use std::{collections::VecDeque, str::FromStr};

use derive_more::derive::From;
use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

type Value = u64;

const COUNTER_KEY: &str = "global-counter";

fn seq_kv_node_id() -> NodeId {
    NodeId::from_str("seq-kv").unwrap()
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct AddBody {
    msg_id: MessageId,
    delta: Value,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct ReadBody {
    msg_id: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize, From)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    #[from]
    Add(AddBody),
    #[from]
    Read(ReadBody),
    UpdateValue {
        value: Value,
    },
    #[serde(rename = "read_ok")]
    KVReadOk {
        in_reply_to: MessageId,
        value: Value,
    },
    #[serde(rename = "cas_ok")]
    KVCompareAndSwapOk {
        in_reply_to: MessageId,
    },
    #[serde(rename = "error")]
    KVError {
        in_reply_to: MessageId,
        code: u32,
        text: String,
    },
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    AddOk {
        msg_id: MessageId,
        in_reply_to: MessageId,
    },
    ReadOk {
        msg_id: MessageId,
        in_reply_to: MessageId,
        value: Value,
    },
    UpdateValue {
        value: Value,
    },
    #[serde(rename = "read")]
    KVRead {
        msg_id: MessageId,
        key: String,
    },
    #[serde(rename = "cas")]
    KVCompareAndSwap {
        msg_id: MessageId,
        key: String,
        from: Value,
        to: Value,
        create_if_not_exists: bool,
    },
}

trait State {
    fn handle(
        self: Box<Self>,
        request: Message<RequestBody>,
        node: &mut GCounterNode,
    ) -> Box<dyn State>;
}

struct DefaultState;

struct AddDelta {
    request: Message<AddBody>,
    block_until_reply: MessageId,
}

struct ReadValue {
    block_until_reply: MessageId,
}

struct GCounterNode {
    id: NodeId,
    other_nodes: Vec<NodeId>,
    msg_ids: MessageIdGenerator,
    value: Option<Value>,
    backlog: VecDeque<Message<RequestBody>>,
    outbox: VecDeque<Message<ResponseBody>>,
    state: Option<Box<dyn State>>,
}

impl InitializedNode for GCounterNode {
    type RequestBody = RequestBody;
    type ResponseBody = ResponseBody;

    fn new(id: NodeId, all_nodes: Box<[NodeId]>) -> Self {
        let other_nodes = all_nodes.iter().copied().filter(|id_| id != *id_).collect();
        Self {
            id,
            other_nodes,
            msg_ids: MessageIdGenerator::default(),
            value: None,
            backlog: VecDeque::new(),
            outbox: VecDeque::new(),
            state: Some(Box::new(DefaultState)),
        }
    }

    fn handle(&mut self, request: Message<RequestBody>) -> Vec<Message<ResponseBody>> {
        let state = self.state.take().expect("state should be set");
        self.state = Some(state.handle(request, self));
        self.outbox.drain(..).collect()
    }
}

impl GCounterNode {
    fn send(&mut self, dest: NodeId, body: ResponseBody) {
        self.outbox.push_back(Message {
            src: self.id,
            dest,
            body,
        })
    }

    fn send_kv_read(&mut self) -> MessageId {
        let msg_id = self.msg_ids.next_id();
        self.send(
            seq_kv_node_id(),
            ResponseBody::KVRead {
                msg_id,
                key: COUNTER_KEY.to_owned(),
            },
        );
        msg_id
    }

    fn reply_reads(&mut self) {
        let value = self.value.expect("value should have been set before");
        let reads: Vec<_> = {
            let first_non_read = self
                .backlog
                .iter()
                .position(|m| !matches!(m.body, RequestBody::Read(_)));
            if let Some(idx) = first_non_read {
                self.backlog.drain(0..idx).collect()
            } else {
                self.backlog.drain(..).collect()
            }
        };
        for read in reads {
            let RequestBody::Read(body) = read.body else {
                // We've drained only matching items.
                unreachable!();
            };
            let msg_id = self.msg_ids.next_id();
            self.send(
                read.src,
                ResponseBody::ReadOk {
                    msg_id,
                    in_reply_to: body.msg_id,
                    value,
                },
            );
        }
    }

    fn process_backlog(&mut self) -> Box<dyn State> {
        let mut new_state: Box<dyn State> = Box::new(DefaultState);
        if let Some(backlog_request) = self.backlog.pop_front() {
            new_state = new_state.handle(backlog_request, self);
        }
        new_state
    }

    fn broadcast_update(&mut self) {
        let node_ids = self.other_nodes.clone();
        for dest in node_ids {
            self.send(
                dest,
                ResponseBody::UpdateValue {
                    value: self.value.expect("value should have been set"),
                },
            );
        }
    }

    fn update_value(&mut self, value: Value) {
        if Some(value) > self.value {
            self.value = Some(value);
        }
    }
}

impl State for DefaultState {
    fn handle(
        self: Box<Self>,
        request: Message<RequestBody>,
        node: &mut GCounterNode,
    ) -> Box<dyn State> {
        use RequestBody::*;
        match &request.body {
            Add(body) => {
                if let Some(value) = node.value {
                    let new_value = value + body.delta;
                    Box::new(AddDelta {
                        request: Message {
                            src: request.src,
                            dest: request.dest,
                            body: body.clone(),
                        },
                        block_until_reply: self.send_kv_cas(value, new_value, node),
                    })
                } else {
                    node.backlog.push_front(request);
                    Box::new(ReadValue {
                        block_until_reply: node.send_kv_read(),
                    })
                }
            }
            Read { .. } => {
                node.backlog.push_front(request);
                Box::new(ReadValue {
                    block_until_reply: node.send_kv_read(),
                })
            }
            UpdateValue { value } => {
                node.update_value(*value);
                self
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}

impl DefaultState {
    fn send_kv_cas(&self, from: Value, to: Value, node: &mut GCounterNode) -> MessageId {
        let msg_id = node.msg_ids.next_id();
        node.send(
            seq_kv_node_id(),
            ResponseBody::KVCompareAndSwap {
                msg_id,
                key: COUNTER_KEY.to_owned(),
                from,
                to,
                create_if_not_exists: true,
            },
        );
        msg_id
    }
}

impl State for AddDelta {
    fn handle(
        self: Box<Self>,
        request: Message<RequestBody>,
        node: &mut GCounterNode,
    ) -> Box<dyn State> {
        use RequestBody::*;
        match &request.body {
            KVCompareAndSwapOk { in_reply_to } if *in_reply_to == self.block_until_reply => {
                let new_value =
                    node.value.expect("value should have been set") + self.request.body.delta;
                node.value = Some(new_value);
                let msg_id = node.msg_ids.next_id();
                node.send(
                    self.request.src,
                    ResponseBody::AddOk {
                        msg_id,
                        in_reply_to: self.request.body.msg_id,
                    },
                );
                node.broadcast_update();
                node.reply_reads();
                node.process_backlog()
            }
            KVError {
                in_reply_to,
                code,
                text: _,
            } if *in_reply_to == self.block_until_reply && *code == 22 => {
                node.backlog.push_front(self.request.mapped());
                Box::new(ReadValue {
                    block_until_reply: node.send_kv_read(),
                })
            }
            Add { .. } | Read { .. } => {
                node.backlog.push_back(request);
                self
            }
            UpdateValue { value } => {
                node.update_value(*value);
                self
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}

impl State for ReadValue {
    fn handle(
        self: Box<Self>,
        request: Message<RequestBody>,
        node: &mut GCounterNode,
    ) -> Box<dyn State> {
        use RequestBody::*;
        match &request.body {
            KVReadOk { in_reply_to, value } if *in_reply_to == self.block_until_reply => {
                node.update_value(*value);
                node.reply_reads();
                node.process_backlog()
            }
            KVError {
                in_reply_to,
                code,
                text: _,
            } if *in_reply_to == self.block_until_reply && *code == 20 => {
                node.value = Some(0);
                node.reply_reads();
                node.process_backlog()
            }
            Add { .. } | Read { .. } => {
                node.backlog.push_back(request);
                self
            }
            UpdateValue { value, .. } => {
                node.update_value(*value);
                self
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    run_node::<GCounterNode>()
}
