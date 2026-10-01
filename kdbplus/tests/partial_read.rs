//! A response that arrives in more than one TCP read, against a fake q server.
#![cfg(feature = "ipc")]

use kdbplus::ipc::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Response message holding the long atom 42: header (little endian, response, uncompressed) + body.
fn response_42() -> Vec<u8> {
    let mut body = vec![0xf9u8]; // -7h: long atom
    body.extend_from_slice(&42i64.to_le_bytes());
    let mut msg = vec![1u8, 2, 0, 0];
    msg.extend_from_slice(&((8 + body.len()) as u32).to_le_bytes());
    msg.extend_from_slice(&body);
    msg
}

/// Fake q: handshake, read one query, then send `chunks` of the response with a pause between,
/// and close the connection afterwards.
async fn fake_q(chunks: Vec<Vec<u8>>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut b = [0u8; 1];
        loop {
            s.read_exact(&mut b).await.unwrap();
            if b[0] == 0 {
                break;
            }
        }
        s.write_all(&[3]).await.unwrap();
        let mut header = [0u8; 8];
        s.read_exact(&mut header).await.unwrap();
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let mut rest = vec![0u8; len - 8];
        s.read_exact(&mut rest).await.unwrap();
        for c in chunks {
            s.write_all(&c).await.unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
    port
}

async fn query(port: u16) -> Result<K> {
    let mut q = QStream::connect(ConnectionMethod::TCP, "127.0.0.1", port, "u:p").await?;
    q.send_sync_message(&"x").await
}

#[tokio::test]
async fn response_in_one_read() {
    let port = fake_q(vec![response_42()]).await;
    let k = tokio::time::timeout(Duration::from_secs(3), query(port))
        .await
        .expect("hung")
        .unwrap();
    assert_eq!(k.get_long().unwrap(), 42);
}

#[tokio::test]
async fn response_body_split_across_reads() {
    let m = response_42();
    let port = fake_q(vec![m[..12].to_vec(), m[12..].to_vec()]).await;
    let k = tokio::time::timeout(Duration::from_secs(3), query(port))
        .await
        .expect("hung")
        .unwrap();
    assert_eq!(k.get_long().unwrap(), 42, "got {}", k);
}

#[tokio::test]
async fn connection_closed_mid_body_is_an_error() {
    let m = response_42();
    let port = fake_q(vec![m[..12].to_vec()]).await;
    let r = tokio::time::timeout(Duration::from_secs(3), query(port))
        .await
        .expect("hung on EOF");
    assert!(r.is_err());
}
