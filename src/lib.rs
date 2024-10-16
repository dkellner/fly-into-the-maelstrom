//! # Fly into the Maelstrom
//!
//! A framework for the
//! [distributed systems challenges by fly.io][fly_dist_sys].
//!
//! [fly_dist_sys]: https://fly.io/dist-sys/
//!
//! # Example Usage
//!
//! As the first challenge is a "getting started guide", there's not much to
//! spoil and we can use it as an example:
//!
//! ```no_run
//! use anyhow::Result;
//! use fly_into_the_maelstrom::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Deserialize)]
//! #[serde(tag = "type", rename_all = "snake_case")]
//! enum RequestPayload {
//!     Echo { echo: String },
//! }
//!
//! #[derive(Clone, Debug, Serialize)]
//! #[serde(tag = "type", rename_all = "snake_case")]
//! enum ResponsePayload {
//!     EchoOk { echo: String },
//! }
//!
//! #[derive(Debug)]
//! struct EchoNode {
//!     tx: MessageTransmitter<ResponsePayload>,
//! }
//!
//! impl NodeState for EchoNode {
//!     fn handle(mut self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>> {
//!         let Message {
//!             header,
//!             payload: RequestPayload::Echo { echo },
//!         } = deserialize_message(request)?;
//!         self.tx.reply(&header, ResponsePayload::EchoOk { echo });
//!         Ok(self)
//!     }
//!
//!     fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>> {
//!         Ok(self)
//!     }
//! }
//!
//! fn main() -> anyhow::Result<()> {
//!     run_node(Box::new(|_, tx| Box::new(EchoNode { tx: tx.into() })))
//! }
//! ```

mod init;
mod input;
mod logging;
mod message;
mod node_id;
mod output;

use std::{panic, process, sync::Arc, time::Instant};

use anyhow::Result;

pub use init::*;
use input::{spawn_input_threads, NodeInput};
pub use logging::*;
pub use message::*;
pub use node_id::*;
use output::spawn_output_thread;

/// A node's state (as in state machine).
pub trait NodeState {
    /// Handles an incoming message.
    ///
    /// Typically you want to [deserialize_message] the `request`, match on its
    /// payload and then send one or more messages using [MessageTransmitter].
    fn handle(self: Box<Self>, request: &str) -> Result<Box<dyn NodeState>>;

    /// Handles a previously scheduled wake up call.
    fn wake_up(self: Box<Self>) -> Result<Box<dyn NodeState>>;

    /// Requests or cancels a wake up call.
    ///
    /// [run_node] will call this method *after* each call to
    /// [NodeState::handle] or [NodeState::wake_up] and will call
    /// [NodeState::wake_up] shortly after the returned `Instant`.
    ///
    /// There is at most one active wake up timer. Returning `None` cancels any
    /// active timer. This means you have to repeatedly return a timestamp if
    /// you still want the wakeup to happen.
    ///
    /// Example: If you want either [NodeState::handle]/[NodeState::wake_up] to
    /// be called *at least every 200ms*, just return `Some(Instant::now() +
    /// Duration::from_millis(200))` every time.
    fn next_wake_up(&self) -> Option<Instant> {
        None
    }
}

/// Runs the main loop.
///
/// This will spawn three long-running threads for (1) reading from STDIN, (2)
/// writing to STDOUT and (3) handling wake-up requests from the node.
pub fn run_node(after_init: AfterInitTransition) -> anyhow::Result<()> {
    set_up_panic_handler();
    let logger = Arc::new(Logger::default());

    let (node_rx, wake_up_tx) = spawn_input_threads(Arc::clone(&logger));
    let stdout_tx = spawn_output_thread(Arc::clone(&logger));

    let mut node: Box<dyn NodeState> = Box::new(InitializingNode::new(stdout_tx, after_init));
    loop {
        node = match node_rx.recv()? {
            NodeInput::Message(message) => node.handle(&message)?,
            NodeInput::WakeUp => node.wake_up()?,
        };
        wake_up_tx.send(node.next_wake_up())?;
    }
}

/// Exit the whole process when a thread panics.
fn set_up_panic_handler() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        orig_hook(panic_info);
        process::exit(1);
    }));
}
