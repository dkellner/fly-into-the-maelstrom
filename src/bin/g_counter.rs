use std::collections::VecDeque;

use anyhow::{anyhow, Result};
use derive_more::derive::From;
use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

type Value = u64;

#[derive(PartialEq, Eq, Clone, Debug, Deserialize)]
struct AddPayload {
    delta: Value,
}

#[derive(PartialEq, Eq, Clone, Debug, Deserialize, From)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    #[from]
    Add(AddPayload),
    Read,
    UpdateValue {
        value: Value,
    },
    #[serde(rename = "read_ok")]
    KVReadOk {
        value: Value,
    },
    #[serde(rename = "cas_ok")]
    KVCompareAndSwapOk,
    #[serde(rename = "error")]
    KVError {
        code: KVErrorCode,
        text: String,
    },
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    AddOk,
    ReadOk {
        value: Value,
    },
    UpdateValue {
        value: Value,
    },
    #[serde(rename = "read")]
    KVRead {
        key: String,
    },
    #[serde(rename = "cas")]
    KVCompareAndSwap {
        key: String,
        from: Value,
        to: Value,
        create_if_not_exists: bool,
    },
}

/// Common fields for all states.
struct Common {
    other_nodes: Box<[NodeId]>,
    tx: MessageTransmitter<ResponsePayload>,
    value: Value,
    backlog: VecDeque<Message<RequestPayload>>,
}

impl Common {
    fn update_value(&mut self, value: Value) {
        if value > self.value {
            self.value = value;
        }
    }

    fn reply_reads(&mut self) -> Result<()> {
        let reads: Vec<_> = {
            let first_non_read = self
                .backlog
                .iter()
                .position(|m| !matches!(m.payload, RequestPayload::Read));
            if let Some(idx) = first_non_read {
                self.backlog.drain(0..idx).collect()
            } else {
                self.backlog.drain(..).collect()
            }
        };
        for read in reads {
            let RequestPayload::Read = read.payload else {
                // We've drained only matching items.
                unreachable!();
            };
            self.tx
                .reply(&read.header, ResponsePayload::ReadOk { value: self.value });
        }
        Ok(())
    }
}

struct DefaultState {
    common: Box<Common>,
}

impl NodeState for DefaultState {
    fn handle(self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        let request = deserialize_message(request)?;
        self.handle_request(request)
    }

    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
        Ok(self)
    }
}

impl DefaultState {
    fn new(id: NodeId, all_nodes: &[NodeId], tx: MessageTransmitter<ResponsePayload>) -> Self {
        let other_nodes = all_nodes.iter().copied().filter(|id_| id != *id_).collect();
        let common = Box::new(Common {
            other_nodes,
            tx,
            value: Value::default(),
            backlog: VecDeque::new(),
        });
        Self { common }
    }

    fn handle_request(
        mut self: Box<Self>,
        request: Message<RequestPayload>,
    ) -> Result<Box<dyn NodeState>> {
        use RequestPayload::*;
        let Message { header, payload } = request;
        match payload {
            Add(payload) => {
                let value = self.common.value;
                let new_value = value + payload.delta;
                let block_until_reply = send_kv_cas(value, new_value, &mut self.common.tx);
                Ok(Box::new(AddDelta {
                    common: self.common,
                    request: Message { header, payload },
                    block_until_reply,
                }))
            }
            Read { .. } => {
                self.common.backlog.push_front(Message { header, payload });
                Ok(Box::new(ReadValue {
                    block_until_reply: send_kv_read(&mut self.common.tx),
                    common: self.common,
                }))
            }
            UpdateValue { value } => {
                self.common.update_value(value);
                Ok(self)
            }
            other => Err(anyhow!("unexpected message: {other:?}")),
        }
    }
}

struct AddDelta {
    common: Box<Common>,
    request: Message<AddPayload>,
    block_until_reply: MessageId,
}

impl AddDelta {
    fn broadcast_update(&mut self) -> Result<()> {
        let node_ids = self.common.other_nodes.clone();
        for dest in node_ids {
            self.common.tx.send(
                dest,
                ResponsePayload::UpdateValue {
                    value: self.common.value,
                },
            );
        }
        Ok(())
    }
}

impl NodeState for AddDelta {
    fn handle(mut self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        use RequestPayload::*;
        let Message { header, payload } = deserialize_message(request)?;
        match payload {
            KVCompareAndSwapOk if header.in_reply_to == Some(self.block_until_reply) => {
                let new_value = self.common.value + self.request.payload.delta;
                self.common.value = new_value;
                self.common
                    .tx
                    .reply(&self.request.header, ResponsePayload::AddOk);
                self.broadcast_update()?;
                self.common.reply_reads()?;
                process_next_backlog_request(self.common)
            }
            KVError { code, text: _ }
                if header.in_reply_to == Some(self.block_until_reply)
                    && code == KVErrorCode::PreconditionFailed =>
            {
                self.common.backlog.push_front(self.request.mapped());
                Ok(Box::new(ReadValue {
                    block_until_reply: send_kv_read(&mut self.common.tx),
                    common: self.common,
                }))
            }
            Add { .. } | Read { .. } => {
                self.common.backlog.push_back(Message { header, payload });
                Ok(self)
            }
            UpdateValue { value } => {
                self.common.update_value(value);
                Ok(self)
            }
            other => Err(anyhow!("unexpected message: {other:?}")),
        }
    }

    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
        Ok(self)
    }
}

struct ReadValue {
    common: Box<Common>,
    block_until_reply: MessageId,
}

impl NodeState for ReadValue {
    fn handle(mut self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        use RequestPayload::*;
        let Message { header, payload } = deserialize_message(request)?;
        match payload {
            KVReadOk { value } if header.in_reply_to == Some(self.block_until_reply) => {
                self.common.update_value(value);
                self.common.reply_reads()?;
                process_next_backlog_request(self.common)
            }
            KVError { code, text: _ }
                if header.in_reply_to == Some(self.block_until_reply)
                    && code == KVErrorCode::KeyDoesNotExist =>
            {
                self.common.reply_reads()?;
                process_next_backlog_request(self.common)
            }
            Add { .. } | Read { .. } => {
                self.common.backlog.push_back(Message { header, payload });
                Ok(self)
            }
            UpdateValue { value, .. } => {
                self.common.update_value(value);
                Ok(self)
            }
            other => Err(anyhow!("unexpected message: {other:?}")),
        }
    }

    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
        Ok(self)
    }
}

fn process_next_backlog_request(common: Box<Common>) -> Result<Box<dyn NodeState>> {
    let mut state = Box::new(DefaultState { common });
    if let Some(backlog_request) = state.common.backlog.pop_front() {
        state.handle_request(backlog_request)
    } else {
        Ok(state)
    }
}

const COUNTER_KEY: &str = "global-counter";

// XXX: This really needs const Option::unwrap().
const SEQ_KV_NODE_ID: NodeId = match NodeId::from_str("seq-kv") {
    Ok(node_id) => node_id,
    Err(_) => unreachable!(),
};

#[derive(PartialEq, Eq, Clone, Copy, Debug, Deserialize)]
#[serde(from = "u32")]
enum KVErrorCode {
    KeyDoesNotExist,
    PreconditionFailed,
    Unknown(u32),
}

impl From<u32> for KVErrorCode {
    fn from(source: u32) -> Self {
        match source {
            20 => KVErrorCode::KeyDoesNotExist,
            22 => KVErrorCode::PreconditionFailed,
            other => KVErrorCode::Unknown(other),
        }
    }
}

fn send_kv_read(tx: &mut MessageTransmitter<ResponsePayload>) -> MessageId {
    tx.send(
        SEQ_KV_NODE_ID,
        ResponsePayload::KVRead {
            key: COUNTER_KEY.to_owned(),
        },
    )
}

fn send_kv_cas(from: Value, to: Value, tx: &mut MessageTransmitter<ResponsePayload>) -> MessageId {
    tx.send(
        SEQ_KV_NODE_ID,
        ResponsePayload::KVCompareAndSwap {
            key: COUNTER_KEY.to_owned(),
            from,
            to,
            create_if_not_exists: true,
        },
    )
}

fn main() -> anyhow::Result<()> {
    run_node(Box::new(|init, tx| {
        Box::new(DefaultState::new(init.node_id, &init.node_ids, tx.into()))
    }))
}
