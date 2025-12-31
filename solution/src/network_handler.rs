use crate::domain::*;
use crate::sector_registry::SectorRegistry;
use crate::transfer_public::deserialize_register_command;
use crate::transfer_private::tcp_client_response;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

pub async fn start_server(
    listener : TcpListener,
    mut self_channel : mpsc::Receiver<SystemRegisterCommand>,
    registry: Arc<SectorRegistry>,
    hmac_system_key: [u8; 64],
    hmac_client_key: [u8; 32],
) {
    let tcp_registry = registry.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, _addr)) => {
                    let registry_clone = tcp_registry.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(
                            socket,
                            registry_clone,
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
    });
    let channel_registry = registry.clone();
    tokio::spawn(async move {
        while let Some(cmd) = self_channel.recv().await {
            let channel_reg_clone = channel_registry.clone();
            tokio::spawn(async move {
                let arc_mutex = channel_reg_clone
                    .get_register(cmd.header.sector_idx).await;
                let mut guard = (*arc_mutex).lock().await;
                guard.system_command(cmd).await;
            });
        }
    });
}

async fn handle_connection(
    socket: TcpStream,
    registry: Arc<SectorRegistry>,
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> std::io::Result<()> {
    let (mut read_half, write_half) = socket.into_split();
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
                    if let RegisterCommand::Client(c) = cmd {
                        let mut w_guard = writer.lock().await;
                        let response = ClientCommandResponse {
                            request_identifier: c.header.request_identifier,
                            status: StatusCode::AuthFailure, 
                            op_return: OperationReturn::Write,
                        };
                        if let Err(e) = tcp_client_response(&mut *w_guard, &response, hmac_client_key).await {
                            log::debug!("Failed to send auth failure response: {}", e);
                        }
                        todo!("Don't we need to check here whether the connection still exists??");
                    }
                    
                    //"In case of receiving incorrect message, the connection should be dropped." 
                    //todo! make sure this return satisfies this
                    return Ok(());
                }

                let sector_idx = match &cmd {
                    RegisterCommand::Client(c) => c.header.sector_idx,
                    RegisterCommand::System(s) => s.header.sector_idx,
                };

                //Lazy load register
                let register = registry.get_register(sector_idx).await;
                let mut guard = register.lock().await;

                match cmd {
                    RegisterCommand::System(sys_cmd) => guard.system_command(sys_cmd).await,
                    RegisterCommand::Client(client_cmd) => {
                        let w_clone = writer.clone();
                        let hmac_copy = *hmac_client_key;
                        guard.client_command(client_cmd, Box::new(move |response| Box::pin(async move {
                            let mut w_guard = w_clone.lock().await;
                            let _ = tcp_client_response(&mut *w_guard, &response, &hmac_copy).await;
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

