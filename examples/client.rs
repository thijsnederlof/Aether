use crate::Client::SomeNotification;
use crate::Server::Pong;
use aether::common::{Deserializable, Serializable};
use aether::request_reply_aether_connection::{
    AetherRequestReplyMessage, RequestReplyAetherConnection,
};
use bytes::{BufMut, BytesMut};
use std::io;
use std::time::Duration;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let stream = TcpStream::connect("127.0.0.1:30000").await?;

    let (mut reader_handle, mut writer_handle) =
        RequestReplyAetherConnection::<Client, Server>::start(stream);

    // Handle incoming requests from server in a separate task
    tokio::spawn(async move {
        while let Some(message) = reader_handle.read().await {
            match message {
                AetherRequestReplyMessage::Notify(_payload) => {
                    // handle notify message types from server here. These don't require responses.
                }
                AetherRequestReplyMessage::Request(_payload, _response_channel) => {
                    // handle request message types from server here. These do require responses.
                }
            }
        }

        // If we get here the connection is broken.
    });

    // Send requests to server in a loop
    loop {
        // Send a notification to the server.
        if writer_handle.notify(SomeNotification(42)).await.is_err() {
            // There was a problem sending the notification. Most likely the connection is closed.
        }

        // Send a request to the server which requires response. We will block sending new requests until the response is received.
        // note: accept the response from the response_channel in a new Tokio task to make it non-blocking.
        match writer_handle.request(Client::Ping).await {
            Ok(response_channel) => {
                // Our response will come in via the response_channel
                match response_channel.await {
                    Ok(message) => {
                        match message {
                            Server::Pong => {
                                // we have got the response to our pong request

                                println!("Pong!");
                            }
                        }
                    }
                    Err(_err) => {
                        // There was a problem while receiving a response to our request. Most likely the connection is closed.
                    }
                }
            }
            Err(_err) => {
                // There was a problem sending the request.
            }
        }

        // For this example sleep for two seconds to not overload the server
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// This is what the server can send us
pub enum Server {
    Pong,
}

// Define message identifiers for incoming server messages
const PONG_ID: u8 = 0;

// This is what the client can send
enum Client {
    SomeNotification(u32),
    Ping,
}

// Define message identifiers so that the server can identify the message
const SOME_NOTIFICATION_ID: u8 = 0;
const PING_ID: u8 = 1;

// Implement the Serializable trait for Client to write our messages to the server
impl Serializable for Client {
    fn serialize(&self, buf: &mut BytesMut) {
        match self {
            SomeNotification(some_number) => {
                buf.put_u8(SOME_NOTIFICATION_ID);
                buf.put_u32_le(*some_number);
            }
            Client::Ping => {
                buf.put_u8(PING_ID);
            }
        }
    }
}

// Implement the Deserialize trait for Server to read incoming messages
impl Deserializable for Server {
    fn deserialize(frame: &[u8]) -> Self {
        return match frame[0] {
            PONG_ID => Pong,
            _ => {
                panic!("Unknown message received");
            }
        };
    }
}
