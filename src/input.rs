use std::{
    io::BufRead as _,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread::{sleep, JoinHandle},
    time::{Duration, Instant},
};

use crate::Logger;

pub(crate) enum NodeInput {
    Message(String),
    WakeUp,
}

pub(crate) fn spawn_input_threads(
    logger: Arc<Logger>,
) -> (mpsc::Receiver<NodeInput>, mpsc::SyncSender<Option<Instant>>) {
    let (node_tx, node_rx) = mpsc::sync_channel::<NodeInput>(100);
    let (wake_up_tx, wake_up_rx) = mpsc::sync_channel::<Option<Instant>>(100);
    std::thread::spawn({
        let node_tx = node_tx.clone();
        let logger = Arc::clone(&logger);
        move || stdin_reader(node_tx, logger)
    });
    std::thread::spawn(move || wake_up_handler(wake_up_rx, node_tx, logger));
    (node_rx, wake_up_tx)
}

fn stdin_reader(node_tx: mpsc::SyncSender<NodeInput>, logger: Arc<Logger>) {
    let lines = std::io::stdin().lock().lines();
    for line in lines {
        let line = line.expect("reading from stdin should succeed");
        logger.log(&format!("< {line}"));
        node_tx
            .send(NodeInput::Message(line))
            .expect("sending to channel should succeed");
    }
}

fn wake_up_handler(
    wake_up_rx: mpsc::Receiver<Option<Instant>>,
    node_tx: mpsc::SyncSender<NodeInput>,
    logger: Arc<Logger>,
) {
    let mut waker: Option<WakerHandle> = None;
    loop {
        let next_wake_up = wake_up_rx
            .recv()
            .expect("reading from channel should succeed");
        waker.take();

        if let Some(instant) = next_wake_up {
            let sleep_duration = instant - Instant::now();
            if sleep_duration > Duration::ZERO {
                waker = Some(spawn_waker_thread(
                    sleep_duration,
                    node_tx.clone(),
                    Arc::clone(&logger),
                ));
            } else {
                node_tx
                    .send(NodeInput::WakeUp)
                    .expect("sending wake up should succeed");
            }
        }
    }
}

fn spawn_waker_thread(
    sleep_duration: Duration,
    tx: mpsc::SyncSender<NodeInput>,
    logger: Arc<Logger>,
) -> WakerHandle {
    let active = Arc::new(AtomicBool::new(true));
    let handle = std::thread::spawn({
        let active = Arc::clone(&active);
        move || {
            sleep(sleep_duration);
            if active.load(Ordering::Relaxed) {
                logger.log("< WAKE UP");
                tx.send(NodeInput::WakeUp)
                    .expect("sending wake up should succeed");
            } else {
                logger.log(": (wake up was cancelled)");
            }
        }
    });
    WakerHandle {
        _handle: handle,
        active,
    }
}

struct WakerHandle {
    _handle: JoinHandle<()>,
    active: Arc<AtomicBool>,
}

impl Drop for WakerHandle {
    fn drop(&mut self) {
        // Dropping its JoinHandle does not stop the waker thread. We set
        // active to false to stop it from sending a wake up signal.
        self.active.store(false, Ordering::Relaxed);
    }
}
