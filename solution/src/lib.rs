use crate::sector_registry::SectorRegistry;
use crate::register_client_public::TcpHandler;

//My own modules that do the heavy lifting (except domain)
mod sector_registry;
mod network_handler;
mod transfer_private;
mod domain;

pub use atomic_register_public::{AtomicRegister, build_atomic_register};
pub use register_client_public::{RegisterClient, Broadcast, Send}; 
pub use sectors_manager_public::{SectorsManager, build_sectors_manager};
pub use transfer_public::{serialize_register_command, deserialize_register_command};
pub use domain::*;

const CHANNEL_SIZE : usize = 4096;

//We try to use as little as possible "use" keywords in lib.rs so that other exports are more
//readable
//I'm not sure if this is a good practice tho, or just over engeneering??
pub async fn run_register_process(config: Configuration) {
    let (host, port) = &config.public.tcp_locations[(config.public.self_rank - 1) as usize];
    let address = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("TCP 300ms timeout!");

    //this is a channel for internal in-process communication
    let (process_tx, process_rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);

    let sectors_manager = build_sectors_manager(
        config.public.storage_dir.clone()
    ).await;

    let register_client = std::sync::Arc::new(TcpHandler::new(
        config.public.tcp_locations.clone(),
        config.public.self_rank,
        process_tx,
        config.hmac_system_key,
    ));

    let registry = std::sync::Arc::new(SectorRegistry::new(
        sectors_manager,
        register_client,
        config.public.self_rank,
        config.public.tcp_locations.clone(),
    ));

    network_handler::start_server(
        listener,
        process_rx,
        registry,
        //config.public.tcp_locations,//we don't use config no more so we can pass the ownership
        //config.public.self_rank,
        config.hmac_system_key,
        config.hmac_client_key,
    ).await;
}

pub mod atomic_register_public;
pub mod transfer_public;
pub mod sectors_manager_public;
pub mod register_client_public;
