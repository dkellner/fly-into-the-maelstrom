use std::ops::RangeFrom;

use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
struct UniqueId(NodeId, u64);

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct GenerateBody {
    msg_id: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct GenerateOkBody {
    msg_id: MessageId,
    in_reply_to: MessageId,
    id: UniqueId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestBody {
    Generate(GenerateBody),
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseBody {
    GenerateOk(GenerateOkBody),
}

#[derive(Debug)]
struct UniqueIdsNode {
    id: NodeId,
    msg_ids: MessageIdIter,
    internal_ids: RangeFrom<u64>,
}

impl UniqueIdsNode {
    fn next_unique_id(&mut self) -> UniqueId {
        UniqueId(
            self.id,
            self.internal_ids
                .next()
                .expect("exhausted available internal ids"),
        )
    }
}

impl InitializedNode for UniqueIdsNode {
    type RequestBody = RequestBody;
    type ResponseBody = ResponseBody;

    fn new(id: NodeId, _all_nodes: Box<[NodeId]>) -> Self {
        Self {
            id,
            msg_ids: MessageIdIter::default(),
            internal_ids: 0..,
        }
    }

    fn handle(&mut self, request: Message<RequestBody>) -> Vec<Message<ResponseBody>> {
        let RequestBody::Generate(body) = request.body;
        vec![Message {
            src: self.id,
            dest: request.src,
            body: ResponseBody::GenerateOk(GenerateOkBody {
                msg_id: self
                    .msg_ids
                    .next()
                    .expect("exhausted available message ids"),
                in_reply_to: body.msg_id,
                id: self.next_unique_id(),
            }),
        }]
    }
}

fn main() -> anyhow::Result<()> {
    run_node::<UniqueIdsNode>()
}
