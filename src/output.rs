use std::{
    io::Write,
    sync::{mpsc, Arc},
};

use crate::Logger;

pub(crate) fn spawn_output_thread(logger: Arc<Logger>) -> mpsc::SyncSender<String> {
    let (tx, rx) = mpsc::sync_channel(100);
    std::thread::spawn(move || stdout_writer(rx, logger));
    tx
}

pub fn stdout_writer(rx: mpsc::Receiver<String>, logger: Arc<Logger>) {
    // This thread keeps a lock on stdout to prevent us from accidentally
    // writing to stdout from other threads.
    let mut stdout = std::io::stdout().lock();
    loop {
        let message = rx.recv().expect("reading from channel should succeed");

        // One message per line:
        assert!(!message.contains('\n')); // XXX: move invariant to type, e.g. `OneLineString`
        stdout
            .write_all(message.as_bytes())
            .expect("stdout should be writable");
        stdout.write_all(b"\n").expect("stdout should be writable");

        logger.log(&format!("> {message}")); // logs to stderr
    }
}
