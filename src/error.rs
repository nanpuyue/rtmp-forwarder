use std::fmt::{self, Display, Formatter};
use std::{error, result};

pub type Result<T> = result::Result<T, Error>;

pub trait IntoError {}

pub trait Context<T> {
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Display + Send + 'static;
}

#[derive(Debug)]
pub enum Error {
    Context(String, Box<dyn error::Error + Send>),
    Boxed(Box<dyn error::Error + Send>),
    String(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Context(d, e) => write!(f, "{d}: {e}"),
            Self::String(s) => s.fmt(f),
            Self::Boxed(e) => e.fmt(f),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Context(_, e) => Some(e.as_ref()),
            Error::Boxed(e) => Some(e.as_ref()),
            Error::String(_) => None,
        }
    }
}

impl<T, E> Context<T> for result::Result<T, E>
where
    E: error::Error + Send + 'static,
{
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Display + Send + 'static,
    {
        self.map_err(|e| Error::Context(context.to_string(), Box::new(e)))
    }
}

impl<T> Context<T> for Option<T> {
    fn context<C>(self, context: C) -> Result<T>
    where
        C: Display + Send + 'static,
    {
        self.ok_or(Error::String(context.to_string()))
    }
}

impl<E> From<E> for Error
where
    E: 'static + error::Error + Send + IntoError,
{
    fn from(e: E) -> Self {
        Self::Boxed(Box::new(e))
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::String(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::String(s.to_owned())
    }
}

impl IntoError for std::io::Error {}
impl IntoError for tokio::time::error::Elapsed {}
impl IntoError for std::str::Utf8Error {}
impl IntoError for serde_json::Error {}
impl<T> IntoError for tokio::sync::mpsc::error::SendError<T> {}
