use std::io::Write;

use serde::{Deserialize, Serialize};

use crate::{InitializedNode, Message, MessageId, NodeId};

/// Reads the init message and responds with init_ok.
pub(crate) fn initialize_node<N: InitializedNode>(
    line: &str,
    mut out: impl Write,
) -> anyhow::Result<N> {
    let request: Message<RequestBody> = serde_json::from_str(line)?;
    let RequestBody::Init(init) = request.body;

    let response = Message {
        src: init.node_id,
        dest: request.src,
        body: ResponseBody::InitOk(InitOkBody {
            in_reply_to: init.msg_id,
        }),
    };
    serde_json::to_writer(&mut out, &response)?;
    out.write_all(&[b'\n'])?;

    Ok(N::new(init.node_id, init.node_ids))
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Init(InitBody),
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct InitBody {
    msg_id: MessageId,
    node_id: NodeId,
    node_ids: Box<[NodeId]>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    InitOk(InitOkBody),
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct InitOkBody {
    in_reply_to: MessageId,
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
        let message: Message<RequestBody> = serde_json::from_str(json_string).unwrap();
        assert_eq!(message.src.to_string(), "c1".to_owned());
        let RequestBody::Init(body) = message.body;
        assert_eq!(
            body.node_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
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
        let message: Result<Message<RequestBody>, _> = serde_json::from_str(json_string);
        assert!(matches!(message, Result::Err(_)), "{message:?}");
    }
}
