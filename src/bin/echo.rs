use anyhow::Result;
use fly_into_the_maelstrom::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RequestPayload {
    Echo { echo: String },
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponsePayload {
    EchoOk { echo: String },
}

#[derive(Debug)]
struct EchoNode {
    tx: MessageTransmitter<ResponsePayload>,
}

impl NodeState for EchoNode {
    fn handle(mut self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
        let Message {
            header,
            payload: RequestPayload::Echo { echo },
        } = deserialize_message(request)?;
        self.tx.reply(&header, ResponsePayload::EchoOk { echo });
        Ok(self)
    }

    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
        Ok(self)
    }
}

fn main() -> anyhow::Result<()> {
    run_node(Box::new(|_, tx| Box::new(EchoNode { tx: tx.into() })))
}
