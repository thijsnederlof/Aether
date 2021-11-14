use crate::common::{Deserializable, Serializable};
use bytes::{Buf, BufMut, BytesMut};
use log::debug;
use std::io;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

pub struct BaseAetherConnection<S, D> {
    write_half: AetherWriteHalf<S>,
    read_half: AetherReadHalf<D>,
}

impl<S, D> BaseAetherConnection<S, D> {
    pub fn new(stream: TcpStream) -> BaseAetherConnection<S, D> {
        let (reader, writer) = stream.into_split();

        BaseAetherConnection {
            write_half: AetherWriteHalf {
                writer,
                out_buf: BytesMut::with_capacity(4096),
                serializable: Default::default(),
            },
            read_half: AetherReadHalf {
                reader,
                in_buf: BytesMut::with_capacity(4096),
                deserializable: Default::default(),
            },
        }
    }

    pub fn split(self) -> (AetherReadHalf<D>, AetherWriteHalf<S>) {
        (self.read_half, self.write_half)
    }
}

pub struct AetherWriteHalf<S> {
    out_buf: BytesMut,
    writer: OwnedWriteHalf,
    serializable: PhantomData<S>,
}

pub struct AetherReadHalf<D> {
    in_buf: BytesMut,
    reader: OwnedReadHalf,
    deserializable: PhantomData<D>,
}

impl<S> AetherWriteHalf<S>
where
    S: Serializable,
{
    pub async fn write(&mut self, writeable: S) -> Result<(), io::Error> {
        self.out_buf.put_u16_le(0);

        writeable.serialize(&mut self.out_buf);

        let frame_len = (self.out_buf.len() - 2) as u16;
        let frame_len_split: [u8; 2] = u16::to_le_bytes(frame_len);

        self.out_buf[0] = frame_len_split[0];
        self.out_buf[1] = frame_len_split[1];

        self.writer.write_buf(&mut self.out_buf).await?;

        Ok(())
    }
}

impl<D> AetherReadHalf<D>
where
    D: Deserializable,
{
    pub async fn read(&mut self) -> Result<D, io::Error> {
        self.read_until_buf_contains_at_least(2).await?;

        let frame_len = self.in_buf.get_u16_le() as usize;

        self.read_until_buf_contains_at_least(frame_len).await?;

        let frame = &self.in_buf[..frame_len];
        let parsed_frame = D::deserialize(frame);

        let _ = &self.in_buf.advance(frame_len);

        Ok(parsed_frame)
    }

    async fn read_until_buf_contains_at_least(&mut self, size: usize) -> Result<(), io::Error> {
        while self.in_buf.len() < size {
            if 0 == self.reader.read_buf(&mut self.in_buf).await? {
                return if self.in_buf.is_empty() {
                    debug!("Connection was closed with empty buffer");

                    Err(Error::new(
                        ErrorKind::ConnectionAborted,
                        "Connection was closed",
                    ))
                } else {
                    debug!("Connection was closed with data still in buffer");

                    Err(Error::new(
                        ErrorKind::Interrupted,
                        "Connection was interrupted",
                    ))
                };
            }
        }

        Ok(())
    }
}
