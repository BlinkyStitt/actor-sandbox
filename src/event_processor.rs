use futures::StreamExt;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct EventReceiver<IncomingEvent>(
    #[allow(clippy::type_complexity)]
    pub  flume::Receiver<(
        IncomingEvent,
        oneshot::Sender<Result<IncomingEvent, (IncomingEvent, eyre::Report)>>,
    )>,
);

#[derive(Clone, Debug)]
pub struct EventSender<IncomingEvent>(
    #[allow(clippy::type_complexity)]
    pub  flume::Sender<(
        IncomingEvent,
        oneshot::Sender<Result<IncomingEvent, (IncomingEvent, eyre::Report)>>,
    )>,
);

/// TODO: what should we call this? is this an actor?
/// there should be one and only one of these
#[derive(Clone)]
pub struct EventProcessor<IncomingEvent> {
    pub event_rx: EventReceiver<IncomingEvent>,
    /// The maximum number of events that can be processed concurrently.
    /// This should probably be around or equal to the postgres connection limit.
    /// A lot of the application logic is happening inside a transaction, so we can't have too many at once.
    pub max_concurrency: usize,
    pub shutdown_token: CancellationToken,
}

impl<IncomingEvent: std::fmt::Debug + Send + Sync + 'static> EventProcessor<IncomingEvent> {
    pub fn new(
        max_concurrency: usize,
        shutdown_token: CancellationToken,
    ) -> (Self, EventSender<IncomingEvent>) {
        // TODO: think about this more. no point in having an unbounded number of futures spawned when we have limits on the semaphores
        // TODO: what capacity? unbounded is always scary. but bounded cap is usually just a guess
        // TODO: calculate cap based on configured RAM usage and some napkin math
        // TODO: we want this to be able to buffer off the network. the sender might kick us off if we read too slowly! reconnects there would be good, too!
        let (event_tx, event_rx) = flume::bounded(max_concurrency * 1000);

        let event_rx = EventReceiver(event_rx);
        let event_tx = EventSender(event_tx);

        let x = Self {
            event_rx,
            shutdown_token,
            max_concurrency,
        };

        (x, event_tx)
    }

    pub async fn run(self) -> eyre::Result<()> {
        // TODO: select on this and the shutdown token
        // TODO: for a minute i thought we could just spawn in incoming events. but that doesn't give us any back pressure at all
        // TODO: i kind of want to just spawn an unbounded number here. but i think we should instead have this be a buffered_unordered
        // theres no point in doing more futures than we have semaphores

        // TODO: is child_token or clone cheaper?
        let take_until_token = self.shutdown_token.child_token();

        self.event_rx
            .0
            .into_stream()
            .take_until(async move { take_until_token.cancelled().await })
            .for_each_concurrent(self.max_concurrency, |(event, completed)| {
                let shutdown_token = self.shutdown_token.clone();

                async move {
                    let f = handle_event(event);

                    match tokio::spawn(f).await {
                        Ok(x @ Ok(_)) => {
                            completed.send(x).unwrap();
                        }
                        Ok(x @ Err(_)) => {
                            eprintln!("error in event processor! {:?}", x);
                            shutdown_token.cancel();

                            completed.send(x).unwrap();
                        }
                        Err(err) => {
                            panic!("join error in event processor! {:?}", err);
                        }
                    }
                }
            })
            .await;

        Ok(())
    }
}

pub async fn handle_event<IncomingEvent: std::fmt::Debug>(
    event: IncomingEvent,
) -> Result<IncomingEvent, (IncomingEvent, eyre::Report)> {
    // connect to postgres and insert some rows for deduplication of events
    // TODO: why postgres? is that really the best system for this? i think we need ACID

    // once we know this event is unique, we can send it to the webhook

    // if sending to the webhook failed, send it to the queue to be retried later

    println!("event: {:?}", event);

    Ok(event)
}
