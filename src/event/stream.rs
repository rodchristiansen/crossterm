use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, SyncSender},
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};

use futures_core::stream::Stream;

use crate::event::{
    Event,
    filter::{EventFilter, Filter},
    internal::{self, InternalEvent},
    sys::Waker,
};

/// A stream of `Result<Event>`.
///
/// **This type is not available by default. You have to use the `event-stream` feature flag
/// to make it available.**
///
/// It implements the [Stream](futures_core::stream::Stream)
/// trait and allows you to receive [`Event`]s with [`smol`](https://crates.io/crates/smol)
/// or [`tokio`](https://crates.io/crates/tokio) crates.
///
/// Check the [examples](https://github.com/crossterm-rs/crossterm/tree/master/examples) folder to see how to use
/// it (`event-stream-*`).
#[derive(Debug)]
pub struct EventStream {
    poll_internal_waker: Waker,
    stream_wake_task_executed: Arc<AtomicBool>,
    stream_wake_task_should_shutdown: Arc<AtomicBool>,
    task_sender: SyncSender<Task>,
    terminal_reports: bool,
}

impl Default for EventStream {
    fn default() -> Self {
        Self::with_terminal_reports(false)
    }
}

impl EventStream {
    fn with_terminal_reports(terminal_reports: bool) -> Self {
        let (task_sender, receiver) = mpsc::sync_channel::<Task>(1);

        thread::spawn(move || {
            while let Ok(task) = receiver.recv() {
                loop {
                    if let Ok(true) = internal::poll(None, &StreamFilter(terminal_reports)) {
                        break;
                    }

                    if task.stream_wake_task_should_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                }
                task.stream_wake_task_executed
                    .store(false, Ordering::SeqCst);
                task.stream_waker.wake();
            }
        });

        EventStream {
            poll_internal_waker: internal::lock_event_reader().waker(),
            stream_wake_task_executed: Arc::new(AtomicBool::new(false)),
            stream_wake_task_should_shutdown: Arc::new(AtomicBool::new(false)),
            task_sender,
            terminal_reports,
        }
    }
}

impl EventStream {
    /// Constructs a new instance of `EventStream`.
    pub fn new() -> EventStream {
        EventStream::default()
    }
}

struct Task {
    stream_waker: std::task::Waker,
    stream_wake_task_executed: Arc<AtomicBool>,
    stream_wake_task_should_shutdown: Arc<AtomicBool>,
}

// Note to future me
//
// We need two wakers in order to implement EventStream correctly.
//
// 1. futures::Stream waker
//
// Stream::poll_next can return Poll::Pending which means that there's no
// event available. We are going to spawn a thread with the
// poll_internal(None, &EventFilter) call. This call blocks until an
// event is available and then we have to wake up the executor with notification
// that the task can be resumed.
//
// 2. poll_internal waker
//
// There's no event available, Poll::Pending was returned, stream waker thread
// is up and sitting in the poll_internal. User wants to drop the EventStream.
// We have to wake up the poll_internal (force it to return Ok(false)) and quit
// the thread before we drop.
impl EventStream {
    fn poll_internal(&self, cx: &mut Context<'_>) -> Poll<Option<io::Result<InternalEvent>>> {
        let filter = StreamFilter(self.terminal_reports);
        match internal::poll(Some(Duration::ZERO), &filter) {
            Ok(true) => Poll::Ready(Some(internal::read(&filter))),
            Ok(false) => {
                if !self
                    .stream_wake_task_executed
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    // https://github.com/rust-lang/rust/issues/80486#issuecomment-752244166
                    .unwrap_or_else(|x| x)
                {
                    let stream_waker = cx.waker().clone();
                    let stream_wake_task_executed = self.stream_wake_task_executed.clone();
                    let stream_wake_task_should_shutdown =
                        self.stream_wake_task_should_shutdown.clone();

                    stream_wake_task_should_shutdown.store(false, Ordering::SeqCst);

                    let _ = self.task_sender.send(Task {
                        stream_waker,
                        stream_wake_task_executed,
                        stream_wake_task_should_shutdown,
                    });
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl Stream for EventStream {
    type Item = io::Result<Event>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_internal(cx).map(|event| {
            event.map(|event| {
                event.map(|event| match event {
                    InternalEvent::Event(event) => event,
                    #[cfg(unix)]
                    _ => unreachable!(),
                })
            })
        })
    }
}

#[derive(Clone)]
struct StreamFilter(bool);

impl Filter for StreamFilter {
    fn eval(&self, event: &InternalEvent) -> bool {
        #[cfg(unix)]
        if self.0
            && matches!(
                event,
                InternalEvent::OscColor { .. } | InternalEvent::ColorSchemeChanged { .. }
            )
        {
            return true;
        }
        EventFilter.eval(event)
    }
}

/// Input and terminal appearance reports delivered by the same input reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalEvent {
    /// An ordinary keyboard, paste, mouse, focus, or resize event.
    Input(Event),
    /// A terminal's default foreground (slot 10) or background (slot 11).
    /// An unrecognized payload is represented by `None`.
    DefaultColor { slot: u8, rgb: Option<(u8, u8, u8)> },
    /// A DECSET 2031 notification. Query OSC 10/11 to obtain the actual colors.
    ColorSchemeChanged { dark: bool },
}

/// An opt-in event stream including OSC 10/11 and DECSET 2031 reports on Unix.
///
/// Use this instead of `EventStream`, never concurrently with another input reader.
/// It does not issue queries or enable terminal reporting modes itself.
#[derive(Debug)]
pub struct TerminalEventStream(EventStream);

impl Default for TerminalEventStream {
    fn default() -> Self {
        Self(EventStream::with_terminal_reports(true))
    }
}

impl TerminalEventStream {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Stream for TerminalEventStream {
    type Item = io::Result<TerminalEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_internal(cx).map(|event| {
            event.map(|event| {
                event.map(|event| match event {
                    InternalEvent::Event(event) => TerminalEvent::Input(event),
                    #[cfg(unix)]
                    InternalEvent::OscColor { slot, payload } => TerminalEvent::DefaultColor {
                        slot,
                        rgb: match payload {
                            super::internal::OscColorPayload::Rgb { r, g, b } => Some((r, g, b)),
                            super::internal::OscColorPayload::Unrecognized(_) => None,
                        },
                    },
                    #[cfg(unix)]
                    InternalEvent::ColorSchemeChanged { dark } => {
                        TerminalEvent::ColorSchemeChanged { dark }
                    }
                    #[cfg(unix)]
                    _ => unreachable!(),
                })
            })
        })
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        self.stream_wake_task_should_shutdown
            .store(true, Ordering::SeqCst);
        let _ = self.poll_internal_waker.wake();
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::event::internal::OscColorPayload;

    #[test]
    fn appearance_reports_are_opt_in_and_wake_the_terminal_stream() {
        for event in [
            InternalEvent::OscColor {
                slot: 10,
                payload: OscColorPayload::Rgb { r: 1, g: 2, b: 3 },
            },
            InternalEvent::ColorSchemeChanged { dark: true },
        ] {
            assert!(!StreamFilter(false).eval(&event));
            assert!(StreamFilter(true).eval(&event));
        }
        assert!(!StreamFilter(true).eval(&InternalEvent::CursorPosition(1, 2)));
        let key = InternalEvent::Event(Event::Key(super::super::KeyCode::Char('a').into()));
        assert!(StreamFilter(false).eval(&key));
        assert!(StreamFilter(true).eval(&key));
    }
}
