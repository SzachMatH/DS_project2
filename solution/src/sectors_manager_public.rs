use crate::domain::*;

use crate::{SectorIdx, SectorVec};
use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[async_trait::async_trait]
pub trait SectorsManager: Send + Sync {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec;

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
}

//this is for compile-time error-elimination. Cool, right?
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Timestamp(u64);
//btw, this is zero-cost abstraction!
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct WriteRank(u8);

struct SecMan {
    map : Mutex<HashMap<SectorIdx, (Timestamp, WriteRank)>>,
    root_path : PathBuf
}

#[async_trait::async_trait]
impl SectorsManager for SecMan {
    async fn read_data(&self, idx : SectorIdx) -> SectorVec {
        let mut file = {
            let lock = self.map.lock().await;
            let (timestamp,writerank) = match lock.get(&idx) {
                Some(pair_ts_wr) => *pair_ts_wr,
                None => { return SectorVec(Box::new(serde_big_array::Array([0; SECTOR_SIZE]))); },
            };
            //With this format we can easily use split method
            let file_path = self.root_path.join(
                format!("sec_{}_{}_{}",idx,timestamp.0,writerank.0)
            );
            File::open(file_path).await.expect("read_data fails in File::open")
        };
        let mut read_into : SectorVec = SectorVec(Box::new(serde_big_array::Array([0; SECTOR_SIZE])));
        file.read_exact(&mut read_into.0.as_mut().0).await.expect("read_data fails in read_exact");

        read_into
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {

    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {

    }
}

//this function is also resposible for recovery
/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    unimplemented!()
}
