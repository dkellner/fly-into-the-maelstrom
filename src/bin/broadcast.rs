use std::collections::HashMap;

use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct BroadcastMessage(u64);

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct BroadcastBody {
    msg_id: MessageId,
    message: BroadcastMessage,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct BroadcastOkBody {
    in_reply_to: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct ReadBody {
    msg_id: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct ReadOkBody {
    in_reply_to: MessageId,
    messages: Box<[BroadcastMessage]>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct TopologyBody {
    msg_id: MessageId,
    topology: HashMap<NodeId, Box<[NodeId]>>,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
struct TopologyOkBody {
    in_reply_to: MessageId,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageBody {
    Broadcast(BroadcastBody),
    BroadcastOk(BroadcastOkBody),
    Read(ReadBody),
    ReadOk(ReadOkBody),
    Topology(TopologyBody),
    TopologyOk(TopologyOkBody),
}

#[derive(Debug)]
struct BroadcastNode {
    id: NodeId,
    topology: HashMap<NodeId, Box<[NodeId]>>,
    msg_ids: MessageIdIter,
    messages: Vec<BroadcastMessage>,
}

impl BroadcastNode {
    fn response(&self, dest: NodeId, body: MessageBody) -> Message<MessageBody> {
        Message {
            src: self.id,
            dest,
            body,
        }
    }

    fn handle_broadcast(
        &mut self,
        src: NodeId,
        body: BroadcastBody,
    ) -> Option<Message<MessageBody>> {
        self.messages.push(body.message);
        Some(self.response(
            src,
            MessageBody::BroadcastOk(BroadcastOkBody {
                in_reply_to: body.msg_id,
            }),
        ))
    }

    fn handle_read(&mut self, src: NodeId, body: ReadBody) -> Option<Message<MessageBody>> {
        Some(self.response(
            src,
            MessageBody::ReadOk(ReadOkBody {
                in_reply_to: body.msg_id,
                messages: self.messages.clone().into_iter().collect(),
            }),
        ))
    }

    fn handle_topology(&mut self, src: NodeId, body: TopologyBody) -> Option<Message<MessageBody>> {
        self.topology = body.topology;
        Some(self.response(
            src,
            MessageBody::TopologyOk(TopologyOkBody {
                in_reply_to: body.msg_id,
            }),
        ))
    }
}

impl InitializedNode for BroadcastNode {
    type RequestBody = MessageBody;
    type ResponseBody = MessageBody;

    fn new(id: NodeId, _all_nodes: Box<[NodeId]>) -> Self {
        Self {
            id,
            topology: HashMap::default(),
            msg_ids: MessageIdIter::default(),
            messages: Vec::new(),
        }
    }

    fn handle(&mut self, request: Message<MessageBody>) -> Option<Message<MessageBody>> {
        use MessageBody::*;
        match request.body {
            Broadcast(body) => self.handle_broadcast(request.src, body),
            Read(body) => self.handle_read(request.src, body),
            Topology(body) => self.handle_topology(request.src, body),
            _ => None,
        }
    }
}

fn main() -> anyhow::Result<()> {
    run_node::<BroadcastNode>()
}
