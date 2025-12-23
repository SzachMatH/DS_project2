pub use crate::domain::*;
use std::sync::Arc;
use crate::sector_registry::SectorRegistry;
use crate::register_client_public::TcpHandler;
use crate::sectors_manager_public::{build_sectors_manager, SectorsManager};

mod sector_registry;
mod network_handler;
mod domain;

pub async fn run_register_process(config: Configuration) {
    //the code at the beginning is so that we don't time out
    //Should it be moved to the network_handler?
    //todo! think about it
    let (host, port) = &config.public.tcp_locations[(config.public.self_rank - 1) as usize];
    let address = format!("{}:{}", host, port);
    let listener = TcpHandler::bind(&address)
        .await
        .expect("TCP 300ms timeout!");

    let register_client = Arc::new(TcpHandler::new(
        config.public.tcp_locations.clone() 
    ));

    let registry = Arc::new(SectorRegistry::new(
        build_sectors_manager(
            config.public.storage_dir.clone()
        ).await,
        register_client,
        config.public.self_rank,
        config.public.tcp_locations,
    ));
    
    network_handler::start_server(
        listener,
        registry,
        config.public.tcp_locations,//we don't use config no more so we can pass the ownership
        config.public.self_rank,
        config.hmac_system_key,
        config.hmac_client_key,
    ).await;
}

pub mod atomic_register_public;
pub mod transfer_public;
pub mod sectors_manager_public;
pub mod register_client_public;
