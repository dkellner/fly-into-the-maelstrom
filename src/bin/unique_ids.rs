use std::ops::RangeFrom;

use anyhow::{anyhow, Result};
use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize)]
struct UniqueId(NodeId, u64);

#[derive(PartialEq, Eq, Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Generate,
}

#[derive(PartialEq, Eq, Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    GenerateOk { id: UniqueId },
}

#[derive(Debug)]
struct UniqueIdsNode {
    id: NodeId,
    tx: MessageTransmitter<ResponsePayload>,
    internal_ids: RangeFrom<u64>,
}

impl UniqueIdsNode {
    fn next_unique_id(&mut self) -> Result<UniqueId> {
        let internal_id = self
            .internal_ids
            .next()
            .ok_or(anyhow!("exhausted available internal ids"))?;
        Ok(UniqueId(self.id, internal_id))
    }
}

impl NodeState for UniqueIdsNode {
    fn handle(mut self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        let Message::<RequestPayload> { header, .. } = deserialize_message(request)?;
        let id = self.next_unique_id()?;
        self.tx.reply(&header, ResponsePayload::GenerateOk { id });
        Ok(self)
    }

    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
        Ok(self)
    }
}

fn main() -> anyhow::Result<()> {
    run_node(Box::new(|init, tx| {
        Box::new(UniqueIdsNode {
            id: init.node_id,
            tx: tx.into(),
            internal_ids: 0..,
        })
    }))
}
