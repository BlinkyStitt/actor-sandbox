use tokio::sync::oneshot;

/// this is intentianally **NOT** clone. we only want one of these running
/// TODO: Rename IncomingEvent to Processed Event so we can use ifferent types?
/// TODO: I'm really not sure about this type
pub struct OrderedEventReceiver<IncomingEvent>(
    #[allow(clippy::type_complexity)]
    pub  flume::Receiver<oneshot::Receiver<Result<IncomingEvent, (IncomingEvent, eyre::Report)>>>,
);

#[derive(Clone, Debug)]
pub struct OrderedEventSender<IncomingEvent>(
    #[allow(clippy::type_complexity)]
    pub  flume::Sender<oneshot::Receiver<Result<IncomingEvent, (IncomingEvent, eyre::Report)>>>,
);

pub struct OrderedEventProcessor<IncomingEvent> {
    ordered_events: OrderedEventReceiver<IncomingEvent>,
}

impl<IncomingEvent: std::fmt::Debug> OrderedEventProcessor<IncomingEvent> {
    pub fn new() -> (Self, OrderedEventSender<IncomingEvent>) {
        // we already have a bounded queue at the front. this should probably be bounded too anyways
        let (tx, rx) = flume::unbounded();

        let x = Self {
            ordered_events: OrderedEventReceiver(rx),
        };

        let handle = OrderedEventSender(tx);

        (x, handle)
    }

    pub async fn run(self) -> eyre::Result<()> {
        while let Ok(event_oneshot) = self.ordered_events.0.recv_async().await {
            match event_oneshot.await? {
                Ok(event) => {
                    println!("processing event: {:?}", event);
                }
                Err((event, result)) => {
                    eprintln!("error processing event: {:?} {:?}", event, result);
                    return Err(result);
                }
            }
        }

        Ok(())
    }
}
