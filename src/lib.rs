use ::bytes::Bytes;
use ::futures::Stream;
use ::pin_project_lite::pin_project;
use ::std::io::Error as IOError;
use ::std::pin::Pin;
use ::std::task::{Context, Poll};
use ::tokio::io::{AsyncRead, AsyncWrite};
use ::tokio_util::io::{ReaderStream, StreamReader};

/// Process a stream of bytes with a custom processor function.
pub async fn process_stream<State, Error>(
    input: impl AsyncRead + Unpin,
    output: &mut (impl AsyncWrite + Unpin + ?Sized),
    processor: impl Fn(Bytes, &mut State) -> Result<Bytes, Error>,
) -> Result<(), IOError>
where
    State: Default,
    Error: Into<IOError>,
{
    ::tokio::io::copy(
        &mut StreamReader::new(ReaderStream::new(input).process(processor)),
        output,
    )
    .await?;
    Ok(())
}

pin_project! {
    struct Processor<S, F, State, Error> {
        #[pin]
        stream: S,
        state: State,
        processor_fn: F,
        _error: ::std::marker::PhantomData<Error>,
    }
}

impl<S, F, State, Error> Processor<S, F, State, Error>
where
    State: Default,
{
    fn new(stream: S, processor_fn: F) -> Self {
        Self {
            stream,
            processor_fn,
            state: Default::default(),
            _error: Default::default(),
        }
    }
}

impl<S, F, State, Error> Stream for Processor<S, F, State, Error>
where
    S: Stream<Item = Result<Bytes, IOError>>,
    F: Fn(Bytes, &mut State) -> Result<Bytes, Error>,
    Error: Into<IOError>,
{
    type Item = Result<Bytes, IOError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(bytes) => match bytes {
                None => Poll::Ready(None),
                Some(bytes) => match bytes {
                    Ok(bytes) => Poll::Ready(Some(
                        (this.processor_fn)(bytes, &mut this.state).map_err(|err| err.into()),
                    )),
                    Err(err) => Poll::Ready(Some(Err(err))),
                },
            },
        }
    }
}

trait Process: Sized + Stream {
    fn process<F, State, Error>(self, processor: F) -> Processor<Self, F, State, Error>
    where
        F: Fn(Bytes, &mut State) -> Result<Bytes, Error>,
        State: Default;
}

impl<S: Stream> Process for S {
    fn process<F, State, Error>(self, processor: F) -> Processor<Self, F, State, Error>
    where
        F: Fn(Bytes, &mut State) -> Result<Bytes, Error>,
        State: Default,
    {
        Processor::new(self, processor)
    }
}
