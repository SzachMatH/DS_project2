use crate::domain::{RegisterCommand, ClientCommandResponse};
use crate::sector_registry::SectorRegistry;
use crate::transfer_public::deserialize_register_command;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Starts the TCP listener loop.
/// Takes ownership of the arguments required to run the network layer.
pub async fn start_server(
    mut listener : TcpStream,
    registry: Arc<SectorRegistry>,
    tcp_locations: Vec<(String, u16)>,
    self_rank: u8,
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
) {
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(
                        socket,
                        registry.clone(),//just a reference
                        &hmac_system_key,
                        &hmac_client_key
                    ).await {
                        log::error!("ERROR: start_server {:?}", e);
                    }
                });
            }
            Err(e) => log::error!("There is a connection error with {:?}",e),
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    registry: Arc<SectorRegistry>,
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> std::io::Result<()> {
    let (mut read_half, write_half) = socket.split();
    let writer = Arc::new(Mutex::new(write_half));

    loop {
        let result = deserialize_register_command(
            &mut read_half,
            hmac_system_key,
            hmac_client_key,
        ).await;

        match result {
            Ok((cmd, auth)) => {
                if !auth {
                    log::warn!("Invalid HMAC given");
                    return Ok(()); 
                }

                let sector_idx = match &cmd {
                    RegisterCommand::Client(c) => c.header.sector_idx,
                    RegisterCommand::System(s) => s.header.sector_idx,
                };

                // Lazy load the register for this sector
                let register = registry.get_register(sector_idx).await;
                let mut guard = register.lock().await;

                // Dispatch Command
                match cmd {
                    todo!("fix what is inside this match! It is badly written!!!");
                    todo!("remember to make the special case for messges inside process");
                    RegisterCommand::System(sys_cmd) => guard.system_command(sys_cmd).await,
                    RegisterCommand::Client(client_cmd) => {
                        let w_clone = writer.clone();
                        let key_copy = hmac_client_key.to_owned(); //Is this really a good idea?
                        guard.client_command(client_cmd, Box::new(move |response| Box::pin(async move {
                            let mut w_guard = w_clone.lock().await;
                            let _ = send_client_response(&mut *w_guard, &response, &key_copy).await;
                        }))).await;
                    }
                }
            }
            Err(crate::transfer_public::DecodingError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),//Someone disconnected
            Err(_) => {
                return Err(std::io::Error::new(std::io::ErrorKind::Other, "Protocol Error"));
            }
        }
    }
}

async fn send_client_response<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    response: &ClientCommandResponse,
    hmac_key: &[u8],
) -> Result<(), std::io::Error> {
    let payload = bincode::serde::encode_to_vec(
        response,
        bincode::config::standard().with_big_endian().with_fixed_int_encoding(),
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
