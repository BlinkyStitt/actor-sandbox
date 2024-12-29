mod event_processor;
mod incoming_events;
mod ordered_event_processor;

use std::{future::Future, time::Duration};

use event_processor::EventProcessor;
use futures::{stream::FuturesUnordered, StreamExt as _};
use incoming_events::IncomingEvents;
use ordered_event_processor::OrderedEventProcessor;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

struct App {
    fs: FuturesUnordered<tokio::task::JoinHandle<eyre::Result<()>>>,
    shutdown_token: CancellationToken,
}

impl App {
    pub fn new() -> Self {
        // TODO: take a Config object? i need a generic pattern for that. hot reload is nice but i think i prefer restarting the app

        let shutdown_token = CancellationToken::new();

        let force_shutdown_token = shutdown_token.clone();
        // TODO: if the shutdown token is cancelled, prepare for a force shutdown of the app after a grace period
        tokio::spawn(async move {
            force_shutdown_token.cancelled().await;
            eprintln!("forcing shutdown in 60 seconds");

            sleep(Duration::from_secs(60)).await;

            std::process::exit(1);
        });

        Self {
            fs: FuturesUnordered::new(),
            shutdown_token,
        }
    }

    /// Spawn a future onto the runtime. This future should run forever unless it encounters an error.
    /// If the future
    pub fn spawn<F>(&mut self, f: F)
    where
        F: Future<Output = eyre::Result<()>> + Send + Sync + 'static,
    {
        self.fs.push(tokio::spawn(f));
    }

    pub async fn join(&mut self) -> eyre::Result<()> {
        let _shutdown_token = self.shutdown_token.clone().drop_guard();

        while let Some(x) = self.fs.next().await {
            match x {
                Ok(Ok(())) => {
                    // if the future completes successfully, we don't need to do anything
                    // TODO: how can we tell which future finished?
                    println!("future finished")
                }
                Ok(Err(e)) => {
                    eprintln!("future exited with an error: {:?}", e);
                    self.shutdown_token.cancel();
                    return Err(e);
                }
                Err(e) => {
                    eprintln!("future exited with a join error: {:?}", e);
                    // this one should never happen. if it does, exit now
                    self.shutdown_token.cancel();
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // TODO: dotenv/config/tracing

    let mut app = App::new();

    // i think we don't need the shutdown token here. it will shut down when the last sender is dropped and all events are cancelled.
    let (ordered_event_processor, ordered_event_tx) = OrderedEventProcessor::new();

    let (event_processor, event_tx) = EventProcessor::new(500, app.shutdown_token.clone());

    // TODO: spawn 4 different incoming events. 2 high priority and 2 low priority
    // TODO: they need their own ordered event processors so that they track streams independently
    // TODO: i'm not sure if they need their own event processors, too. need to think more about priority queues
    let incoming_events = IncomingEvents::new(0, event_tx, ordered_event_tx);

    // // TODO: if cloned above, drop the senders. otherwise, the app won't ever exit!
    // drop(event_tx);
    // drop(ordered_event_tx);

    // start the app
    app.spawn(async move { ordered_event_processor.run().await });
    app.spawn(async move { event_processor.run().await });
    app.spawn(async move { incoming_events.run().await });

    // run the app to completion
    app.join().await
}
