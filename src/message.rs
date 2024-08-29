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

#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MessageId(u64);

#[derive(Debug)]
pub struct MessageIdIter {
    iter: RangeFrom<u64>,
}

impl Default for MessageIdIter {
    fn default() -> Self {
        Self { iter: 0.. }
    }
}

impl Iterator for MessageIdIter {
    type Item = MessageId;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(MessageId)
    }
}
