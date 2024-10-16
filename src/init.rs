use std::sync::mpsc;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{deserialize_message, Message, MessageTransmitter, NodeId, NodeState};

/// Returns the state after the node was successfully initialized.
pub type AfterInitTransition =
    Box<dyn Fn(InitPayload, MessageTransmitter<()>) -> Box<dyn NodeState>>;

pub(crate) struct InitializingNode {
    stdout_tx: mpsc::SyncSender<String>,
    after_init: AfterInitTransition,
}

impl InitializingNode {
    pub(crate) fn new(
        stdout_tx: mpsc::SyncSender<String>,
        after_init: AfterInitTransition,
    ) -> Self {
        Self {
            stdout_tx,
            after_init,
        }
    }
}

impl NodeState for InitializingNode {
    fn handle(self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        let init_message: Message<RequestPayload> = deserialize_message(request)?;

        let Message { header, payload } = init_message;
        let RequestPayload::Init(data) = payload;

        let mut tx = MessageTransmitter::new(data.node_id, self.stdout_tx);
        tx.reply(&header, ResponsePayload::InitOk);

        Ok((self.after_init)(data, tx.into()))
    }

    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
        Ok(self)
    }
}

/// The payload a node received with the `init` message.
#[derive(PartialEq, Eq, Clone, Debug, Deserialize)]
pub struct InitPayload {
    pub node_id: NodeId,
    pub node_ids: Box<[NodeId]>,
}

#[derive(PartialEq, Eq, Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Init(InitPayload),
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    InitOk,
}

#[cfg(test)]
mod tests {
    use crate::Message;

    use super::*;

    #[test]
    fn deserialize_init_message() {
        let json_string = r#"{
            "src": "c1",
            "dest": "n3",
            "body": {
                "type": "init",
                "msg_id": 1,
                "node_id": "n3",
                "node_ids": ["n1", "n2", "n3"]
            }
        }"#;
        let message: Message<RequestPayload> = serde_json::from_str(json_string).unwrap();
        assert_eq!(message.header.src.to_string(), "c1".to_owned());
        let RequestPayload::Init(InitPayload { node_ids, .. }) = message.payload;
        assert_eq!(
            node_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
            vec!["n1".to_owned(), "n2".to_owned(), "n3".to_owned()]
        );
    }

    #[test]
    fn deserialize_init_message_failure() {
        let json_string = r#"{
            "src": "c1",
            "dest": "n3",
            "body": {
                "type": "no-init",
                "msg_id": 1,
                "node_id": "n3",
                "node_ids": ["n1", "n2", "n3"]
            }
        }"#;
        let message: Result<Message<RequestPayload>, _> = serde_json::from_str(json_string);
        assert!(matches!(message, Result::Err(_)), "{message:?}");
    }
}
