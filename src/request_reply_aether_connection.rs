use crate::base_aether_connection::{AetherReadHalf, AetherWriteHalf, BaseAetherConnection};
use crate::common::{Deserializable, Serializable};
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use log::debug;
use std::io;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};

pub struct RequestReplyAetherConnection<REQ, RES> {
    req: PhantomData<REQ>,
    res: PhantomData<RES>,
}

enum MessageRequestType<REQ: Serializable> {
    Notify(REQ),
    Request(u16, REQ),
    Response(u16, REQ),
}

const NOTIFY_TYPE: u8 = 0x00;
const REQUEST_TYPE: u8 = 0x01;
const RESPONSE_TYPE: u8 = 0x02;

impl<RES: Serializable> Serializable for MessageRequestType<RES> {
    fn serialize(&self, buf: &mut BytesMut) {
        match self {
            MessageRequestType::Notify(req) => {
                buf.put_u8(NOTIFY_TYPE);
                req.serialize(buf);
            }
            MessageRequestType::Request(msg_id, req) => {
                buf.put_u8(REQUEST_TYPE);
                buf.put_u16_le(*msg_id);
                req.serialize(buf);
            }
            MessageRequestType::Response(msg_id, req) => {
                buf.put_u8(RESPONSE_TYPE);
                buf.put_u16_le(*msg_id);
                req.serialize(buf);
            }
        }
    }
}

enum MessageReceiveType<RES: Deserializable> {
    Notify(RES),
    Request(u16, RES),
    Response(u16, RES),
}

impl<RES: Deserializable> Deserializable for MessageReceiveType<RES> {
    fn deserialize(frame: &[u8]) -> Self {
        match frame[0] {
            NOTIFY_TYPE => MessageReceiveType::Notify(RES::deserialize(&frame[1..])),
            REQUEST_TYPE => {
                let msg_id = u16::from_le_bytes(frame[1..3].try_into().unwrap());

                MessageReceiveType::Request(msg_id, RES::deserialize(&frame[3..]))
            }
            RESPONSE_TYPE => {
                let msg_id = u16::from_le_bytes(frame[1..3].try_into().unwrap());

                MessageReceiveType::Response(msg_id, RES::deserialize(&frame[3..]))
            }
            _ => {
                debug!("Received unknown packet type with identifier: {}", frame[0]);

                // todo: handle unknown packet types
                panic!("Unknown packet type")
            }
        }
    }
}

pub enum AetherRequestReplyMessage<REQ, RES> {
    Notify(RES),
    Request(RES, oneshot::Sender<REQ>),
}

impl<REQ, RES> RequestReplyAetherConnection<REQ, RES>
where
    REQ: Serializable + Send + 'static,
    RES: Deserializable + Send + 'static,
{
    pub fn start(
        stream: TcpStream,
    ) -> (
        RequestReplyReaderHandle<REQ, RES>,
        RequestReplyWriterHandle<REQ, RES>,
    ) {
        let (reader_half, writer_half) = BaseAetherConnection::new(stream).split();

        let (writer_tx, writer_rx) = mpsc::channel(1500);
        let callbacks = Arc::new(DashMap::new());

        let reader_handle = RequestReplyAetherConnection::start_reader(
            reader_half,
            writer_tx.clone(),
            callbacks.clone(),
        );

        let writer_handle = RequestReplyAetherConnection::start_writer(
            writer_half,
            writer_rx,
            writer_tx,
            callbacks,
        );

        (reader_handle, writer_handle)
    }
}

impl<REQ, RES> RequestReplyAetherConnection<REQ, RES>
where
    REQ: Serializable + Send + 'static,
    RES: Deserializable + Send + 'static,
{
    fn start_reader(
        mut reader_half: AetherReadHalf<MessageReceiveType<RES>>,
        writer_tx: Sender<MessageRequestType<REQ>>,
        callbacks: Arc<DashMap<u16, oneshot::Sender<RES>>>,
    ) -> RequestReplyReaderHandle<REQ, RES> {
        let (reader_tx, reader_rx) = mpsc::channel(1500);

        tokio::spawn(async move {
            loop {
                let result: Result<MessageReceiveType<RES>, io::Error> = reader_half.read().await;

                match result {
                    Ok(message) => {
                        if reader_tx.send(message).await.is_err() {
                            debug!("Read new message but reader channel was closed.");

                            break;
                        }
                    }
                    Err(err) => {
                        debug!("Could not read new message from connection: {}", err);

                        break;
                    }
                }
            }
        });

        RequestReplyReaderHandle {
            reader_rx,
            writer_tx,
            callbacks,
        }
    }

    fn start_writer(
        mut writer_half: AetherWriteHalf<MessageRequestType<REQ>>,
        mut writer_rx: Receiver<MessageRequestType<REQ>>,
        writer_tx: Sender<MessageRequestType<REQ>>,
        callbacks: Arc<DashMap<u16, oneshot::Sender<RES>>>,
    ) -> RequestReplyWriterHandle<REQ, RES> {
        tokio::spawn(async move {
            loop {
                if let Some(req) = writer_rx.recv().await {
                    match writer_half.write(req).await {
                        Ok(_) => {}
                        Err(err) => {
                            debug!("Could not write message: {}", err);

                            break;
                        }
                    }
                }
            }
        });

        RequestReplyWriterHandle {
            sender: writer_tx,
            msg_counter: 0,
            callbacks,
        }
    }
}

pub struct RequestReplyReaderHandle<REQ: Serializable, RES: Deserializable> {
    reader_rx: mpsc::Receiver<MessageReceiveType<RES>>,
    writer_tx: Sender<MessageRequestType<REQ>>,
    callbacks: Arc<DashMap<u16, oneshot::Sender<RES>>>,
}

impl<REQ, RES> RequestReplyReaderHandle<REQ, RES>
where
    REQ: Serializable + Send + 'static,
    RES: Deserializable + Send + 'static,
{
    pub async fn read(&mut self) -> Option<AetherRequestReplyMessage<REQ, RES>> {
        loop {
            match self.reader_rx.recv().await {
                None => return None,
                Some(message) => match message {
                    MessageReceiveType::Notify(response) => {
                        return Some(AetherRequestReplyMessage::Notify(response));
                    }
                    MessageReceiveType::Request(msg_id, response) => {
                        let (response_tx, response_rx) = oneshot::channel();
                        let writer_tx = self.writer_tx.clone();

                        tokio::spawn(async move {
                            if let Ok(value) = response_rx.await {
                                if writer_tx
                                    .send(MessageRequestType::Response(msg_id, value))
                                    .await
                                    .is_err()
                                {
                                    debug!("Could not write response back. Channel was closed.");
                                }
                            }
                        });

                        return Some(AetherRequestReplyMessage::Request(response, response_tx));
                    }
                    MessageReceiveType::Response(msg_id, response) => {
                        let callback = self.callbacks.remove(&msg_id);

                        match callback {
                            None => {}
                            Some((_, sender)) => {
                                tokio::spawn(async move {
                                    if sender.send(response).is_err() {
                                        debug!("Could not supply response to pending callback. Response channel was closed.");
                                    }
                                });
                            }
                        }
                    }
                },
            }
        }
    }
}

pub struct RequestReplyWriterHandle<REQ, RES>
where
    REQ: Serializable,
    RES: Deserializable,
{
    sender: mpsc::Sender<MessageRequestType<REQ>>,
    msg_counter: u16,
    callbacks: Arc<DashMap<u16, oneshot::Sender<RES>>>,
}

impl<REQ, RES> RequestReplyWriterHandle<REQ, RES>
where
    REQ: Serializable,
    RES: Deserializable,
{
    pub async fn notify(&mut self, req: REQ) -> Result<(), io::Error> {
        match self.sender.send(MessageRequestType::Notify(req)).await {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::new(
                ErrorKind::ConnectionAborted,
                "Connection was closed",
            )),
        }
    }

    pub async fn request(&mut self, req: REQ) -> Result<oneshot::Receiver<RES>, io::Error> {
        let (tx, rx) = oneshot::channel();

        let msg_id = self.register_callback(tx).await;

        match self
            .sender
            .send(MessageRequestType::Request(msg_id, req))
            .await
        {
            Ok(()) => Ok(rx),
            Err(_) => Err(Error::new(
                ErrorKind::ConnectionAborted,
                "Connection was closed",
            )),
        }
    }

    async fn register_callback(&mut self, callback: oneshot::Sender<RES>) -> u16 {
        let msg_id = self.msg_counter;

        while self.callbacks.contains_key(&msg_id) {
            tokio::time::sleep(Duration::from_micros(500)).await;
        }

        self.callbacks.insert(msg_id, callback);
        self.msg_counter = self.msg_counter.wrapping_add(1);

        msg_id
    }
}
