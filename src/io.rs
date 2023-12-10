use std::error::Error;
use bytes::{Bytes, BytesMut};
use tokio::{io::{AsyncWriteExt, AsyncReadExt}, sync::Mutex};
use std::sync::Arc;
use tokio::net::TcpStream;

pub(super) mod messages;
pub(super) mod records;
pub(super) mod call_state;

pub(super) struct IO {
    tcp_stream: Arc<Mutex<TcpStream>>
}

impl From<TcpStream> for IO {
    fn from(tcp_stream: TcpStream) -> Self {
        Self {
            tcp_stream: Arc::new(Mutex::new(tcp_stream))
        }
    }
}

impl IO {
    pub async fn call(&mut self, request: BytesMut) -> Result<Bytes, Box<dyn Error>> {
        let mut used_stream = self.tcp_stream.lock().await;

        used_stream.write_all(&request).await?;
        used_stream.flush().await?;

        let response_length = {
            let mut response_length_bytes: [u8; 4] = [0; 4];
            used_stream.read_exact(&mut response_length_bytes).await?;

            i32::from_be_bytes(response_length_bytes) as usize
        };

        let mut response = vec![0u8; response_length];
        used_stream.read_exact(&mut response).await?;

        Ok(Bytes::from(response))
    }

    pub fn with_cloned_arc(&self) -> Self {
        Self {
            tcp_stream: self.tcp_stream.clone(),
        }
    }
}