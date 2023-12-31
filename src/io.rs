use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;
use bytes::{Bytes, BytesMut};

pub(super) mod messages;
pub(super) mod records;
pub(super) mod call_state;

pub(super) struct IO {
    tcp_stream: TcpStream
}

impl From<TcpStream> for IO {
    fn from(tcp_stream: TcpStream) -> Self {
        Self {
            tcp_stream
        }
    }
}

impl IO {
    pub fn call(&mut self, request: BytesMut) -> Result<Bytes, Box<dyn Error>> {
        self.tcp_stream.write_all(&request)?;
        self.tcp_stream.flush()?;

        let response_length = {
            let mut response_length_bytes: [u8; 4] = [0; 4];
            self.tcp_stream.read_exact(&mut response_length_bytes)?;

            i32::from_be_bytes(response_length_bytes) as usize
        };

        let mut response = vec![0u8; response_length];
        self.tcp_stream.read_exact(&mut response)?;

        Ok(Bytes::from(response))
    }

    pub fn try_clone(&self) -> Result<Self, std::io::Error> {
        let result = 
            Self {
                tcp_stream: self.tcp_stream.try_clone()?,
            };

        Ok(result)
    }
}