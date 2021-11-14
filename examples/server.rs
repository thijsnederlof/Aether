use crate::Client::{Ping, SomeNotification};
use crate::Server::Pong;
use aether::common::{Deserializable, Serializable};
use aether::request_reply_aether_connection::{
    AetherRequestReplyMessage, RequestReplyAetherConnection,
};
use bytes::{BufMut, BytesMut};
use std::io;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let listener = TcpListener::bind("127.0.0.1:30000").await?;

    loop {
        let (stream, addr) = listener.accept().await?;

        println!("Received a new connection from: {}", addr);

        let (mut reader_handle, _writer_handle) =
            RequestReplyAetherConnection::<Server, Client>::start(stream);

        // Handle incoming requests from client in a separate task
        tokio::spawn(async move {
            while let Some(message) = reader_handle.read().await {
                match message {
                    AetherRequestReplyMessage::Notify(payload) => {
                        // handle notify message types from client here. These don't require responses.

                        if let SomeNotification(some_number) = payload {
                            println!(
                                "Got SomeNotification notification with number: {}",
                                some_number
                            );
                        }

                        // currently do nothing on other notifications
                    }
                    AetherRequestReplyMessage::Request(payload, response_channel) => {
                        // handle request message types from client here. These do require responses.

                        if let Ping = payload {
                            println!("Ping");

                            // send back the response on the response_channel
                            if response_channel.send(Pong).is_err() {
                                // If it errors then the connection is broken. Add your own handling here.
                            };
                        }
                    }
                }
            }

            // If we get here the connection is broken.
        });

        // Send requests to server in a loop
        loop {
            // Sleep forever so we dont exit the program
            tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
        }
    }
}

// This is what the client can send us
enum Client {
    SomeNotification(u32),
    Ping,
}

// Define message identifiers so that we can see what type of message we are dealing with
const SOME_NOTIFICATION_ID: u8 = 0;
const PING_ID: u8 = 1;

// This is what the server can send
pub enum Server {
    Pong,
}

// Define message identifiers so that the client can identify the message
const PONG_ID: u8 = 0;

// Implement the Serializable trait for Server to write our messages to the client
impl Serializable for Server {
    fn serialize(&self, buf: &mut BytesMut) {
        match self {
            Server::Pong => {
                buf.put_u8(PONG_ID);
            }
        }
    }
}

// Implement the Deserialize trait for Client to read incoming messages
impl Deserializable for Client {
    fn deserialize(frame: &[u8]) -> Self {
        return match frame[0] {
            SOME_NOTIFICATION_ID => {
                let payload = u32::from_le_bytes(frame[1..].try_into().unwrap());

                SomeNotification(payload)
            }
            PING_ID => Ping,
            _ => {
                panic!("Unknown message received");
            }
        };
    }
}
