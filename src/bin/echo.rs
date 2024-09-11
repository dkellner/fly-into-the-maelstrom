use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Echo { msg_id: MessageId, echo: Box<str> },
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    EchoOk {
        msg_id: MessageId,
        in_reply_to: MessageId,
        echo: Box<str>,
    },
}

#[derive(Debug)]
struct EchoNode {
    id: NodeId,
    msg_ids: MessageIdGenerator,
}

impl InitializedNode for EchoNode {
    type RequestBody = RequestBody;
    type ResponseBody = ResponseBody;

    fn new(id: NodeId, _all_nodes: Box<[NodeId]>) -> Self {
        Self {
            id,
            msg_ids: MessageIdGenerator::default(),
        }
    }

    fn handle(&mut self, request: Message<RequestBody>) -> Vec<Message<ResponseBody>> {
        let RequestBody::Echo { msg_id, echo } = request.body;
        let response = Message {
            src: self.id,
            dest: request.src,
            body: ResponseBody::EchoOk {
                msg_id: self.msg_ids.next_id(),
                in_reply_to: msg_id,
                echo,
            },
        };
        vec![response]
    }
}

fn main() -> anyhow::Result<()> {
    run_node::<EchoNode>()
}
