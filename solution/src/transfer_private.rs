use crate::domain::*;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use bincode::serde;
use bincode::config;
use hmac::{Hmac, Mac};
use sha2::Sha256;

//Since we were explicitly asked not to add this to the transfer_public we make a separate module
pub async fn tcp_client_response(
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    response: &ClientCommandResponse,
    hmac_key: &[u8],
) -> Result<(),std::io::Error> {
    let payload = serde::encode_to_vec(
        response,
        config::standard().with_big_endian().with_fixed_int_encoding(),
    ).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(hmac_key)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid Key"))?;
    mac.update(&payload);
    let tag = mac.finalize().into_bytes();

    let msg_len = (payload.len() + tag.len()) as u64;
    writer.write_all(&msg_len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.write_all(&tag).await?;
    writer.flush().await?;
    
    Ok(())
}

//This function is here mostly to keep the logic of the transfering data regardless of the 
//way it is begin transferred
//To be fair this function is really simple, but keeping it here shows the intention of how 
//the design of this app was meant to look like 
pub async fn channel_client_response(
    sender: &Sender<ClientCommandResponse>,
    response: ClientCommandResponse,
) -> Result<(), SendError<ClientCommandResponse>> {
    sender.send(response).await
}
