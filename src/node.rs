use std::io::{BufRead, Write};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{init::initialize_node, Message, NodeId};

pub trait InitializedNode {
    type RequestBody: Serialize + for<'a> Deserialize<'a>;
    type ResponseBody: Serialize + for<'a> Deserialize<'a>;

    fn new(id: NodeId, all_nodes: Box<[NodeId]>) -> Self;
    fn handle(
        &mut self,
        request: Message<Self::RequestBody>,
    ) -> Option<Message<Self::ResponseBody>>;
}

pub fn run_node<N: InitializedNode>() -> anyhow::Result<()> {
    let mut lines = std::io::stdin().lock().lines();
    let mut stdout = std::io::stdout().lock();

    let first_line = lines
        .next()
        .ok_or_else(|| anyhow!("did not receive line"))??;
    let mut node: N = initialize_node(&first_line, &mut stdout)?;

    for line in lines {
        let request: Message<N::RequestBody> = serde_json::from_str(&line?)?;
        if let Some(response) = node.handle(request) {
            serde_json::to_writer(&mut stdout, &response)?;
            stdout.write_all(&[b'\n'])?;
        }
    }

    Ok(())
}
