use std::{collections::HashMap, ops::RangeFrom};

use anyhow::Result;
use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

type LogKey = String;

/// The type of values we are receiving and storing in our logs.
type Value = u64;

type Offset = u64;

#[derive(Debug, Default)]
struct Log {
    entries: Vec<(Offset, Value)>,
    committed_offsets: HashMap<NodeId, Offset>,
}

impl Log {
    fn append(&mut self, value: Value) -> Offset {
        let offset = self.entries.last().map(|e| e.0).unwrap_or_default() + 1;
        self.entries.push((offset, value));
        offset
    }

    fn entries(&self, offset: RangeFrom<Offset>) -> Vec<(Offset, Value)> {
        self.entries
            .iter()
            .skip_while(|(o, _)| *o < offset.start)
            .cloned()
            .collect()
    }

    fn commit(&mut self, client: NodeId, offset: Offset) {
        self.committed_offsets
            .entry(client)
            .and_modify(|o| *o = (*o).max(offset))
            .or_insert(offset);
    }

    fn committed_offset(&self, client: &NodeId) -> Option<Offset> {
        self.committed_offsets.get(client).cloned()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Send {
        key: String,
        #[serde(rename = "msg")]
        value: Value,
    },
    Poll {
        offsets: HashMap<LogKey, Offset>,
    },
    CommitOffsets {
        offsets: HashMap<LogKey, Offset>,
    },
    ListCommittedOffsets {
        keys: Vec<LogKey>,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum ResponsePayload {
    SendOk {
        offset: Offset,
    },
    PollOk {
        #[serde(rename = "msgs")]
        values: HashMap<LogKey, Vec<(Offset, Value)>>,
    },
    CommitOffsetsOk,
    ListCommittedOffsetsOk {
        offsets: HashMap<LogKey, Offset>,
    },
}

#[derive(Debug)]
struct KafkaNode {
    tx: MessageTransmitter<ResponsePayload>,
    logs: HashMap<LogKey, Log>,
}

impl KafkaNode {
    fn new(tx: MessageTransmitter<ResponsePayload>) -> Self {
        Self {
            tx,
            logs: HashMap::default(),
        }
    }

    fn committed_offsets(&self, client: &NodeId, key: LogKey) -> Option<(LogKey, Offset)> {
        let log = self.logs.get(&key)?;
        let offset = log.committed_offset(client)?;
        Some((key, offset))
    }

    fn handle_send(&mut self, header: MessageHeader, key: LogKey, value: Value) {
        let log = self.logs.entry(key).or_default();
        let offset = log.append(value);
        self.tx.reply(&header, ResponsePayload::SendOk { offset });
    }

    fn handle_poll(&mut self, header: MessageHeader, offsets: HashMap<String, u64>) {
        self.tx.reply(
            &header,
            ResponsePayload::PollOk {
                values: offsets
                    .into_iter()
                    .map(|(key, offset)| {
                        (
                            key.clone(),
                            self.logs.entry(key).or_default().entries(offset..),
                        )
                    })
                    .collect(),
            },
        );
    }

    fn handle_commit_offsets(&mut self, header: MessageHeader, offsets: HashMap<String, u64>) {
        for (key, offset) in offsets.into_iter() {
            if let Some(log) = self.logs.get_mut(&key) {
                log.commit(header.src, offset);
            }
        }
        self.tx.reply(&header, ResponsePayload::CommitOffsetsOk);
    }

    fn handle_list_committed_offsets(&mut self, header: MessageHeader, keys: Vec<String>) {
        self.tx.reply(
            &header,
            ResponsePayload::ListCommittedOffsetsOk {
                offsets: keys
                    .into_iter()
                    .filter_map(|key| self.committed_offsets(&header.src, key))
                    .collect(),
            },
        );
    }
}

impl NodeState for KafkaNode {
    fn handle(mut self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        let Message::<RequestPayload> { header, payload } = deserialize_message(request)?;
        use RequestPayload::*;
        match payload {
            Send { key, value } => self.handle_send(header, key, value),
            Poll { offsets } => self.handle_poll(header, offsets),
            CommitOffsets { offsets } => self.handle_commit_offsets(header, offsets),
            ListCommittedOffsets { keys } => self.handle_list_committed_offsets(header, keys),
        };
        Ok(self)
    }

    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
        Ok(self)
    }
}

fn main() -> anyhow::Result<()> {
    run_node(Box::new(|_, tx| Box::new(KafkaNode::new(tx.into()))))
}
