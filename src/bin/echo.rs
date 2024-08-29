use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct EchoBody {
    msg_id: MessageId,
    echo: Box<str>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct EchoOkBody {
    msg_id: MessageId,
    in_reply_to: MessageId,
    echo: Box<str>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Echo(EchoBody),
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    EchoOk(EchoOkBody),
}

#[derive(Debug)]
struct EchoNode {
    id: NodeId,
    msg_ids: MessageIdIter,
}

impl InitializedNode for EchoNode {
    type RequestBody = RequestBody;
    type ResponseBody = ResponseBody;

    fn new(id: NodeId, _all_nodes: Box<[NodeId]>) -> Self {
        Self {
            id,
            msg_ids: MessageIdIter::default(),
        }
    }

    fn handle(&mut self, request: Message<RequestBody>) -> Option<Message<ResponseBody>> {
        let RequestBody::Echo(body) = request.body;
        Some(Message {
            src: self.id,
            dest: request.src,
            body: ResponseBody::EchoOk(EchoOkBody {
                msg_id: self
                    .msg_ids
                    .next()
                    .expect("exhausted available message ids"),
                in_reply_to: body.msg_id,
                echo: body.echo,
            }),
        })
    }
}

fn main() -> anyhow::Result<()> {
    run_node::<EchoNode>()
}
