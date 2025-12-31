use crate::domain::*;

use crate::{SectorIdx, SectorVec};
use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::fs::{self,File};
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
//After completing this assigment I think this was a bad idea
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct WriteRank(u8);

struct SecMan {
    map : Mutex<HashMap<SectorIdx, (Timestamp, WriteRank)>>,
    root_path : PathBuf//path for our direcotry where we store everything
}

impl SecMan {
    fn new(root_dir : PathBuf) -> Self {
        Self {
            root_path : root_dir,
            map : Mutex::new(HashMap::<SectorIdx,(Timestamp, WriteRank)>::new())
        }
    }

    async fn sync_root(&self) -> std::io::Result<()> { //this syncs direcotry, not file!
        let filer = File::open(&self.root_path).await?;
        filer.sync_data().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SectorsManager for SecMan {
    async fn read_data(&self, idx : SectorIdx) -> SectorVec {
        let path_to_read = {
            let lock = self.map.lock().await;
            let (timestamp,writerank) = match lock.get(&idx) {
                Some(pair_ts_wr) => *pair_ts_wr,
                None => { return SectorVec(Box::new(serde_big_array::Array([0; SECTOR_SIZE]))); },
            };
            let file_path = self.root_path.join(
                format!("sec_{}_{}_{}",idx,timestamp.0,writerank.0)
            );
            file_path
        };
        let mut file = File::open(&path_to_read).await.expect("Failed to open file in read_data");
        let mut read_into : SectorVec = SectorVec(Box::new(serde_big_array::Array([0; SECTOR_SIZE])));
        //if let Err(e) = file.read_exact(&mut **read_into.0).await {
        //    log::error!("Sector {} is corrupted! err = {}", idx, e);
        //    return SectorVec(Box::new(serde_big_array::Array([0; SECTOR_SIZE])));
        //}
        file.read_exact(&mut **read_into.0).await.expect("read_data fails in read_exact");

        read_into
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let lock = self.map.lock().await;
        match lock.get(&idx) {
            Some((ts, wr)) => (ts.0, wr.0),
            None => (0, 0),
        }
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        //NOTE TO SELF : Don't do any hash nonsense. Not worth it.
        
        let tmp_path = self.root_path.join(
            format!("tmp_{}_{}_{}",idx, sector.1, sector.2)
        );//owned
        
        //Make tmp file and sync it
        {
            let mut tmp_file = File::create(&tmp_path).await.expect("Failed to create new file");
            tmp_file.write_all(&**sector.0.0).await.expect("Failed to write data");
            tmp_file.sync_data().await.expect("Failed to sync data");
        }

        //Renaming tmp file and sync
        let sector_path = self.root_path.join(
            format!("sec_{}_{}_{}",idx, sector.1, sector.2)
        );
        fs::rename(tmp_path, &sector_path).await.unwrap();//compiler says there is no expect?
        self.sync_root().await.expect("Failed to sync root after rename");
        
        //Here we need a guarantee that after crash-recovery there is always at most one file with
        //the sam idx number
        let old_metadata = {
            let mut lock = self.map.lock().await;
            lock.insert(idx, (Timestamp(sector.1), WriteRank(sector.2)))
        };

        if let Some((Timestamp(old_ts), WriteRank(old_wr))) = old_metadata {
            if (old_ts, old_wr) != (sector.1, sector.2) {
                let old_path = self.root_path.join(
                    format!("sec_{}_{}_{}", idx, old_ts, old_wr)
                );
                //It's okay if the file is already gone.
                let _ = fs::remove_file(old_path).await;
            }
        }

        //Leaving the code below just in case
        ////let err_io_mapper = |e| todo!("Add error handling to this structure");
        //{
        //    let mut file = File::open(&tmp_sec).await.unwrap();//.map_err(err_io_mapper)?;
        //    file.write_all(&verifier).await.unwrap();//map_err(err_io_mapper)?;
        //    //This is crazy...
        //    file.write_all(&**sector.0.0).await.unwrap();//map_err(err_io_mapper)?;
        //    file.sync_data().await.unwrap();//.map(err_io_mapper)?;
        //
        //}
        //self.sync_root().await.unwrap();//todo! handle errors
        //
        //let dest_path = self.root_path.join(
        //    format!("sec_{}_{}_{}",idx, sector.1, sector.2)
        //);
        //let file_existance = dest_path.exists();
        //
        //{
        //    let mut file = File::create(&dest_path).await.unwrap();//todo! handle errors
        //    file.write_all(&**sector.0.0);
        //    file.sync_data().await.unwrap();//handle err todo!
        //}
        //if !file_existance {
        //    self.sync_root().await.unwrap(); //handle err todo!
        //}
        //fs::remove_file(tmp_sec).await.unwrap();//handle err todo!
        //todo!("Here is a good place to add (ts, wr) to the map");
        //self.sync_root().await.unwrap();//handle err todo!
        //
        //self.sync_root().await.unwrap();//todo! handle error this
    }
}

//this function is also resposible for recovery
/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    let mut map = HashMap::new();
    let mut read_dir = fs::read_dir(&path).await.expect("Failed to read directory");

    while let Some(entry) = read_dir.next_entry().await.expect("Failed to read entry") {
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();

        if name_str.starts_with("tmp_") {
            //We don't really need to panic here, but in the future it'd be worth giving a warning
            log::warn!("Removing tmp file after crash {}",name_str);
            let _ = fs::remove_file(entry.path()).await;
            continue;
        } else if name_str.starts_with("sec_") {
            if let Ok(metadata) = fs::metadata(entry.path()).await {
                if metadata.len() != SECTOR_SIZE as u64 {
                    log::error!("Corrupted file found! removing...");
                    let _ = fs::remove_file(entry.path()).await;
                    continue;
                }
            }
            let parts: Vec<&str> = name_str.split('_').collect();
            if let (Ok(idx), Ok(ts), Ok(wr)) = (
                parts[1].parse::<SectorIdx>(),
                parts[2].parse::<u64>(),
                parts[3].parse::<u8>(),
            ) {
                let current_best = map.get(&idx);
                let found_ver = (Timestamp(ts), WriteRank(wr));
                
                match current_best {
                    Some(&existing_ver) => {
                        if found_ver > existing_ver {
                            let (old_ts, old_wr) = existing_ver;
                            let old_path = path.join(format!("sec_{}_{}_{}", idx, old_ts.0, old_wr.0));
                            let _ = fs::remove_file(old_path).await;
                            
                            map.insert(idx, found_ver);
                        } else {
                            let _ = fs::remove_file(entry.path()).await;
                        }
                    },
                    None => {
                        map.insert(idx, found_ver);
                    }
                }
            }
        } else {
            unreachable!("There is some real pathology here...");
        }
    }
    Arc::new(SecMan {
        map : Mutex::new(map),
        root_path : path,
    })
}

