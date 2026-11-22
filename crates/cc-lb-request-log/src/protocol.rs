use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Transport/framing protocol observed for a persisted request event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestProtocol {
    /// Standard HTTP request/response (non-streaming buffered body).
    Http,
    /// HTTP response streamed as Server-Sent Events (`text/event-stream`).
    HttpSse,
    /// HTTP response streamed without SSE framing.
    HttpStream,
    /// WebSocket turn or session (Responses WS, Realtime WS, Gemini Live WS).
    Websocket,
}

impl RequestProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::HttpSse => "http_sse",
            Self::HttpStream => "http_stream",
            Self::Websocket => "websocket",
        }
    }
}

impl fmt::Display for RequestProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseRequestProtocolError {
    raw: String,
}

impl fmt::Display for ParseRequestProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown request protocol: {}", self.raw)
    }
}

impl std::error::Error for ParseRequestProtocolError {}

impl FromStr for RequestProtocol {
    type Err = ParseRequestProtocolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "http" => Ok(Self::Http),
            "http_sse" => Ok(Self::HttpSse),
            "http_stream" => Ok(Self::HttpStream),
            "websocket" => Ok(Self::Websocket),
            _ => Err(ParseRequestProtocolError { raw: s.to_owned() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RequestProtocol;

    #[test]
    fn request_protocol_roundtrips_str_and_json() {
        for (protocol, expected) in [
            (RequestProtocol::Http, "http"),
            (RequestProtocol::HttpSse, "http_sse"),
            (RequestProtocol::HttpStream, "http_stream"),
            (RequestProtocol::Websocket, "websocket"),
        ] {
            assert_eq!(protocol.as_str(), expected);
            assert_eq!(protocol.to_string(), expected);
            assert_eq!(expected.parse::<RequestProtocol>().unwrap(), protocol);
            let json = serde_json::to_value(protocol).unwrap();
            assert_eq!(json, serde_json::Value::String(expected.to_owned()));
            let restored: RequestProtocol = serde_json::from_value(json).unwrap();
            assert_eq!(restored, protocol);
        }
        assert!("bogus".parse::<RequestProtocol>().is_err());
    }
}
