use std::io::{BufRead, Write};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{InitializedNode, Message, MessageId, NodeId};

/// Reads the init message and responds with init_ok.
pub(crate) fn initialize_node<N: InitializedNode>() -> anyhow::Result<N> {
    let first_line = std::io::stdin()
        .lock()
        .lines()
        .next()
        .ok_or_else(|| anyhow!("did not receive line"))??;
    let init_message: Message<InitBody> = serde_json::from_str(&first_line)?;

    let InitBody::Init {
        msg_id,
        node_id,
        node_ids,
    } = init_message.body;

    let response = Message {
        src: node_id,
        dest: init_message.src,
        body: InitOkBody::InitOk {
            in_reply_to: msg_id,
        },
    };

    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &response)?;
    stdout.write_all(&[b'\n'])?;

    Ok(N::new(node_id, node_ids))
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitBody {
    Init {
        msg_id: MessageId,
        node_id: NodeId,
        node_ids: Box<[NodeId]>,
    },
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitOkBody {
    InitOk { in_reply_to: MessageId },
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
        let message: Message<InitBody> = serde_json::from_str(json_string).unwrap();
        assert_eq!(message.src.to_string(), "c1".to_owned());
        let InitBody::Init { node_ids, .. } = message.body;
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
        let message: Result<Message<InitBody>, _> = serde_json::from_str(json_string);
        assert!(matches!(message, Result::Err(_)), "{message:?}");
    }
}
