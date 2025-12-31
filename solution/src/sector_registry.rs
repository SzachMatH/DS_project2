use crate::domain::*;
use crate::atomic_register_public::{build_atomic_register, AtomicRegister};
use crate::sectors_manager_public::SectorsManager;
use crate::register_client_public::RegisterClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

//todo! Write what this module is meant for in this place!

pub struct SectorRegistry {
    active_registers: RwLock<HashMap<SectorIdx, Arc<Mutex<Box<dyn AtomicRegister>>>>>,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<dyn RegisterClient>,
    self_rank: u8,
    process_count: u8,
}

impl SectorRegistry {
    pub fn new(
        sectors_manager: Arc<dyn SectorsManager>,
        register_client: Arc<dyn RegisterClient>,
        config_self_rank : u8,
        config_tcp_locations : Vec<(String, u16)>,
    ) -> Self {
        Self {
            active_registers: RwLock::new(HashMap::new()),
            sectors_manager,
            register_client,
            self_rank: config_self_rank,
            process_count: config_tcp_locations.len() as u8,
        }
    }

    pub async fn get_register(&self, sector_idx: SectorIdx) -> Arc<Mutex<Box<dyn AtomicRegister>>> {
        {
            let read_guard = self.active_registers.read().await;
            if let Some(register) = read_guard.get(&sector_idx) {
                return register.clone();
            }
        }

        //From now on we can assume there is no register with a given SectorId
        let mut write_guard = self.active_registers.write().await;

        //Double-check prevents race conditions
        if let Some(register) = write_guard.get(&sector_idx) {
            return register.clone();
        }

        let new_register = build_atomic_register(
            self.self_rank,
            sector_idx,
            self.register_client.clone(),
            self.sectors_manager.clone(),
            self.process_count,
        ).await;
        let entry = Arc::new(Mutex::new(new_register));
        write_guard.insert(sector_idx, entry.clone());
        entry
    }
}
