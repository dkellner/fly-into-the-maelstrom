use std::{
    io::{BufRead, Write},
    sync::mpsc,
    thread::{sleep, JoinHandle},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{init::initialize_node, Message, NodeId};

enum NodeInput<B> {
    Message(Message<B>),
    WakeUp,
}

pub trait InitializedNode {
    type RequestBody: Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static;
    type ResponseBody: Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static;

    fn new(id: NodeId, all_nodes: Box<[NodeId]>) -> Self;

    fn handle(&mut self, request: Message<Self::RequestBody>) -> Vec<Message<Self::ResponseBody>>;

    fn wake_up(&mut self) -> Vec<Message<Self::ResponseBody>> {
        vec![]
    }

    fn next_wake_up(&self) -> Option<Instant> {
        None
    }
}

pub fn run_node<N: InitializedNode>() -> anyhow::Result<()> {
    let mut node: N = initialize_node()?;

    let (node_tx, node_rx) = mpsc::channel::<NodeInput<N::RequestBody>>();
    let (stdout_tx, stdout_rx) = mpsc::channel::<Message<N::ResponseBody>>();
    let (wake_up_tx, wake_up_rx) = mpsc::channel::<Option<Instant>>();
    let node_tx_2 = node_tx.clone();
    std::thread::spawn(|| read_stdin::<N>(node_tx));
    std::thread::spawn(|| write_stdout::<N>(stdout_rx));
    std::thread::spawn(|| handle_wake_up::<N>(wake_up_rx, node_tx_2));

    loop {
        let responses = match node_rx.recv()? {
            NodeInput::Message(message) => node.handle(message),
            NodeInput::WakeUp => node.wake_up(),
        };
        for response in responses {
            stdout_tx.send(response)?;
        }
        wake_up_tx.send(node.next_wake_up())?;
    }
}

fn read_stdin<N: InitializedNode>(
    node_tx: mpsc::Sender<NodeInput<N::RequestBody>>,
) -> anyhow::Result<()> {
    let lines = std::io::stdin().lock().lines();
    for line in lines {
        let request: Message<N::RequestBody> = serde_json::from_str(&line?)?;
        node_tx.send(NodeInput::Message(request))?;
    }
    Ok(())
}

fn write_stdout<N: InitializedNode>(
    rx: mpsc::Receiver<Message<N::ResponseBody>>,
) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();
    loop {
        let message = rx.recv()?;
        serde_json::to_writer(&mut stdout, &message)?;
        stdout.write_all(&[b'\n'])?;
    }
}

fn handle_wake_up<N: InitializedNode>(
    wake_up_rx: mpsc::Receiver<Option<Instant>>,
    node_tx: mpsc::Sender<NodeInput<N::RequestBody>>,
) -> anyhow::Result<()> {
    let mut waker: Option<JoinHandle<_>> = None;
    loop {
        let next_wake_up = wake_up_rx.recv()?;
        waker.take(); // XXX: kill thread instead of detaching it
        if let Some(instant) = next_wake_up {
            let sleep_duration = instant - Instant::now();
            if sleep_duration > Duration::ZERO {
                let tx = node_tx.clone();
                waker = Some(std::thread::spawn(move || {
                    sleep(instant - Instant::now());
                    tx.send(NodeInput::WakeUp)
                }));
            } else {
                node_tx.send(NodeInput::WakeUp)?;
            }
        }
    }
}
