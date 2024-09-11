use std::ops::RangeFrom;

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// A message following Maelstrom's protocol.
///
/// See https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md .
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Message<B> {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: B,
}

impl<B> Message<B> {
    pub fn mapped<C: From<B>>(self) -> Message<C> {
        Message {
            src: self.src,
            dest: self.dest,
            body: self.body.into(),
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MessageId(u64);

#[derive(Debug)]
pub struct MessageIdGenerator {
    iter: RangeFrom<u64>,
}

impl Default for MessageIdGenerator {
    fn default() -> Self {
        Self { iter: 0.. }
    }
}

impl MessageIdGenerator {
    pub fn next_id(&mut self) -> MessageId {
        self.iter
            .next()
            .map(MessageId)
            .expect("exhausted available message ids")
    }
}
