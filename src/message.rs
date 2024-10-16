use std::{marker::PhantomData, ops::RangeFrom, sync::mpsc};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::NodeId;

/// A message following Maelstrom's protocol.
///
/// See <https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md>.
///
/// Note that we move `msg_id` and `in_reply_to` into [MessageHeader] for
/// practical reasons. Most of our code wants to match on `payload` and does
/// not care about those fields. On the other hand, replying to messages does
/// not need the payload, so this seems to be a more natural structure.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "P: Clone + Serialize",
    deserialize = "P: Deserialize<'de>"
))]
#[serde(from = "MaelstromMessage<P>")]
#[serde(into = "MaelstromMessage<P>")]
pub struct Message<P> {
    pub header: MessageHeader,
    pub payload: P,
}

/// A message's header (everything except the payload).
#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MessageHeader {
    pub src: NodeId,
    pub dest: NodeId,
    pub msg_id: Option<MessageId>,
    pub in_reply_to: Option<MessageId>,
}

impl<P> Message<P> {
    pub fn mapped<Q: From<P>>(self) -> Message<Q> {
        Message {
            header: self.header,
            payload: self.payload.into(),
        }
    }
}

/// A message's unique identifier within a node.
///
/// This identifier is automatically created within [MessageTransmitter],
/// ensuring it gets incremented for each message.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MessageId(u64);

#[derive(Debug)]
struct MessageIdGenerator {
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

/// The only way of sending [Message]s.
///
/// Note that all messages created with [MessageTransmitter] will contain a
/// unique [MessageId]. This is a simplification, Maelstrom actually supports
/// messages without an ID.
///
/// It is the only way of sending messages because:
/// - [crate::run_node] will start a thread that keeps a lock on STDOUT.
/// - This thread reads messages from a channel and outputs them.
/// - The [MessageTransmitter] created in [crate::run_node] contains the
///   *only* sender for this channel.
#[derive(Debug)]
pub struct MessageTransmitter<P> {
    src: NodeId,
    msg_ids: MessageIdGenerator,
    tx: mpsc::SyncSender<String>,
    _payload: PhantomData<P>,
}

impl<P: Clone + Serialize> MessageTransmitter<P> {
    pub(crate) fn new(src: NodeId, tx: mpsc::SyncSender<String>) -> Self {
        Self {
            src,
            msg_ids: MessageIdGenerator::default(),
            tx,
            _payload: PhantomData,
        }
    }

    /// Changes the payload type.
    pub fn into<Q>(self) -> MessageTransmitter<Q> {
        MessageTransmitter {
            src: self.src,
            msg_ids: self.msg_ids,
            tx: self.tx,
            _payload: PhantomData,
        }
    }

    /// Prepares a message for later sending.
    pub fn prepare<Q>(
        &mut self,
        dest: NodeId,
        in_reply_to: Option<MessageId>,
        payload: Q,
    ) -> Message<Q> {
        Message {
            header: MessageHeader {
                src: self.src,
                dest,
                msg_id: Some(self.msg_ids.next_id()),
                in_reply_to,
            },
            payload,
        }
    }

    /// Sends a prepared message.
    pub fn send_message(&mut self, message: &Message<P>) {
        self.tx
            .send(serialize_message(message))
            .expect("sending message should succeed");
    }

    /// Sends a message to `dest` specified by `payload`.
    ///
    /// The message will be assigned a unique [MessageId].
    pub fn send(&mut self, dest: NodeId, payload: P) -> MessageId {
        let message = self.prepare(dest, None, payload);
        self.send_message(&message);
        message.header.msg_id.expect("msg_id should be set")
    }

    /// Sends a reply for another message.
    ///
    /// The message will be assigned a unique [MessageId] and will use the
    /// original message's ID for `in_reply_to`.
    pub fn reply(&mut self, header: &MessageHeader, payload: P) -> MessageId {
        assert!(header.msg_id.is_some());
        let message = self.prepare(header.src, header.msg_id, payload);
        self.send_message(&message);
        message.header.msg_id.expect("msg_id should be set")
    }
}

/// Serializes a message to a JSON string.
pub fn serialize_message<P: Clone + Serialize>(message: &Message<P>) -> String {
    serde_json::to_string(message).expect("message should be serializable")
}

/// Deserializes a message from a JSON string.
pub fn deserialize_message<P: for<'a> Deserialize<'a>>(message: &str) -> Result<Message<P>> {
    serde_json::from_str(message).map_err(Into::into)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MaelstromMessage<P> {
    src: NodeId,
    dest: NodeId,
    body: MaelstromMessageBody<P>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MaelstromMessageBody<P> {
    msg_id: Option<MessageId>,
    in_reply_to: Option<MessageId>,
    #[serde(flatten)]
    payload: P,
}

impl<P> From<MaelstromMessage<P>> for Message<P> {
    fn from(source: MaelstromMessage<P>) -> Self {
        Self {
            header: MessageHeader {
                src: source.src,
                dest: source.dest,
                msg_id: source.body.msg_id,
                in_reply_to: source.body.in_reply_to,
            },
            payload: source.body.payload,
        }
    }
}

impl<P> From<Message<P>> for MaelstromMessage<P> {
    fn from(source: Message<P>) -> Self {
        Self {
            src: source.header.src,
            dest: source.header.dest,
            body: MaelstromMessageBody {
                msg_id: source.header.msg_id,
                in_reply_to: source.header.in_reply_to,
                payload: source.payload,
            },
        }
    }
}
