use tokio::sync::oneshot;

use crate::{event_processor::EventSender, ordered_event_processor::OrderedEventSender};

/// TODO: what should this hold? i think it should have a name for logging purposes
#[derive(Debug)]
pub struct IncomingEvents<IncomingEvent> {
    id: usize,
    /// events are handled here first. they are handled in parallel as quickly as possible.
    /// once completed, the results are sent tothe ordered_event_handle
    event_tx: EventSender<IncomingEvent>,
    /// events are handled here second. they are handled serially in the order they were created.
    /// TODO: should this have a different type? its like a "ProcessedEvent"
    ordered_event_tx: OrderedEventSender<IncomingEvent>,
}

impl<IncomingEvent> IncomingEvents<IncomingEvent> {
    pub fn new(
        id: usize,
        event_tx: EventSender<IncomingEvent>,
        ordered_event_tx: OrderedEventSender<IncomingEvent>,
    ) -> Self {
        Self {
            id,
            event_tx,
            ordered_event_tx,
        }
    }
}

/// TODO: what should we do here? grpc subscription? kafka consumer? s3 object key notifications?
impl IncomingEvents<usize> {
    pub async fn run(self) -> eyre::Result<()> {
        for i in 0..100_000usize {
            let (tx, rx) = oneshot::channel();

            self.ordered_event_tx.0.send_async(rx).await.unwrap();

            // the receiver end of this event_tx runs a bunch of futures in parallel
            self.event_tx.0.send_async((i, tx)).await?;
        }

        Ok(())
    }
}
