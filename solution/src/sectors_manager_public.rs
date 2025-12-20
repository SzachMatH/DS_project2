use crate::domain::*;

use crate::{SectorIdx, SectorVec};
use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::fs::{self,File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sha2::{Digest,Sha256};

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

impl SecMan {
    fn new(root_dir : PathBuf) -> Self {
        Self {
            root_path : root_dir,
            map : Mutex::new(HashMap::<SectorIdx,(Timestamp, WriteRank)>::new())
        }
    }

    async fn sync_root(&self) -> std::io::Result<()> { //this syncs direcotry, not file!
        let filer = File::open(&self.root_storage_dir).await?;
        filer.sync_data().await?;
        Ok(())
    }
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
            let file_path = self.root_path.join(
                format!("sec_{}_{}_{}",idx,timestamp.0,writerank.0)
            );
            todo!("Think whether if this changes the self.root_path itself");
            File::open(file_path).await.expect("read_data fails in File::open")
        };
        let mut read_into : SectorVec = SectorVec(Box::new(serde_big_array::Array([0; SECTOR_SIZE])));
        file.read_exact(&mut **read_into.0).await.expect("read_data fails in read_exact");

        read_into
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let lock = self.map.lock().await;
        let ret = lock[&idx];
        (ret.0.0, ret.1.0)
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {

        let mut hasher = Sha256::new();
        hasher.update(&**sector.0.0);
        let verifier = hasher.finalize();
        let safe_verifier : String /*64b long*/ = verifier.iter()
            .map(|byte| format!("{:02x}", byte))
            .collect();

        let tmp_sec = self.root_path.join(
            format!("tmp-{}_{}_{}_{}",safe_verifier,idx, sector.1, sector.2)
            // => tmp_sec is 64+8+1+7 bytes long < 255B
            // Therefore it fits into the name
        );
        

        //let err_io_mapper = |e| todo!("Add error handling to this structure");
        {
            let mut file = File::open(&tmp_sec).await.unwrap();//.map_err(err_io_mapper)?;
            file.write_all(&verifier).await.unwrap();//map_err(err_io_mapper)?;
            //This is crazy...
            file.write_all(&**sector.0.0).await.unwrap();//map_err(err_io_mapper)?;
            file.sync_data().await.unwrap();//.map(err_io_mapper)?;

        }
        self.sync_root().await.unwrap();//todo! handle errors

        let dest_path = self.root_path.join(
            format!("sec_{}_{}_{}",idx, sector.1, sector.2)
        );
        let file_existance = dest_path.exists();

        {
            let mut file = File::create(&dest_path).await.unwrap();//todo! handle errors
            file.write_all(&**sector.0.0);
            file.sync_data().await.unwrap();//handle err todo!
        }
        if !file_existance {
            self.sync_root().await.unwrap(); //handle err todo!
        }
        fs::remove_file(tmp_sec).await.unwrap();//handle err todo!
        todo!("Here is a good place to add (ts, wr) to the map");
        self.sync_root().await.unwrap();//handle err todo!

        self.sync_root().await.unwrap();//todo! handle error this
        

    }
}

//this function is also resposible for recovery
/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    unimplemented!()
}
