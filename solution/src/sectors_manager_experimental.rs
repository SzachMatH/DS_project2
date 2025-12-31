use crate::domain::*;
use crate::{SectorIdx, SectorVec};
use std::sync::Arc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Timestamp(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct WriteRank(u8);

struct SecMan {
    root_path : PathBuf,
    stable_mem : StableMem,
    map : Mutex<HashMap<SectorIdx,(Timestamp, WriteRank)>>,
}

impl SecMan {
    fn make_map(path : &PathBuf) {
        let mut entries = fs::read_dir(path).await
            .expect("Cannot get the enrites of root");

        let map = HashMap::<SectorIdx, (Timestamp, Writerank)>::new();
        while let Some(entry) = entries.next_entry().await
            .expect("some dir entry is corrupted") 
        {
            //Parsing....
            let file_name = entry.file_name()
                .to_string_lossy();
            let mut iter = file_name.split('_');
            let prefix = iter.next(); // this is either tmp or sec
            if &prefix == "tmp" {
                todo!("think about it");
            }
            let hash = iter.next().to_string();
            let idx = u64::from_str_radix(iter.next(), 16).ok()?; 
            let writerank = u8::from_str_radix(iter.next(), 16).ok()?;
            let timestamp = u64::from_str_radix(iter.next(), 16).ok()?;

            if file_name.starts_with("tmp") {
                let tmp_file_path = entry.path();
                if let Err(e) = fs::remove_file(&tmp_file_path).await {
                    eprintln!("Failed to remove temp file {:?}: {}", path, e);
                }
            }
        }
    }

    fn new(path: PathBuf) -> self {
        SecMan {
            root_path : path,//It is here so that ownership preserves hierarchy logic
            stable_mem : StableMem::build_stable_storage(&path), // Is there a lot to build?
            map : Mutex::new(make_map(&path)),
        }
    }
}

impl SectorsManager for SecMan {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let (timestamp, writerank) = {
            let guard = self.map.lock();
            guard[idx]
        };
        let mut read_into : SectorVec = SectorVec(Box::new(serde_big_array::Array([0; SECTOR_SIZE])));

        match self.stable_mem.read(timestamp, writerank) {
            None => SectorVec(Box::new(
                serde_big_array::Array([0; SECTOR_SIZE]))),
            Some(data) => SectorVec(Box::new(
                serde_big_array::Array(data))),
        }
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let guard = self.map.lock();
        if let Some(ret) = guard[&idx] {
            (ret.0, ret.1)
        } else {
            (0,0)
        }
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let (ts, wr) = {
            let guard = self.map.lock();
            guard[&idx]
        }; // this parenthesis are used so that we drop the guard

        todo!("What if the timestamp is the same. Is that possible?");
        let tmp_path = todo!();
        self.stable_mem.put(tmp_path,&**sector.0);
    }
}

struct StableMem {
    root_storage_dir : PathBuf,
}

impl StableMem {
    pub fn new(root_dir : PathBuf) -> Self {
        let mut index = HashMap::new();
        let mut dir = fs::read_dir(&root_path).await.expect("root dir is broken");

        while let Ok(Some(entry)) = dir.next_entry().await {
            let fname = entry.file_name().to_string_lossy().to_string();
            
            //sec_{idx}_{ts}_{wr}
            if !fname.starts_with("sec_") { continue; }
            let parts: Vec<&str> = fname.split('_').collect();
            if parts.len() != 4 { continue; }

            if let (Ok(idx), Ok(ts), Ok(wr)) = (
                parts[1].parse::<SectorIdx>(),//todo! shouldn this be a different type?
                parts[2].parse::<u64>(),
                parts[3].parse::<u8>(),
            ) {
                let current = index.entry(idx).or_insert((0, 0, String::new()));
                if (ts, wr) > (current.0, current.1) {
                    *current = (ts, wr, fname.clone());
                    // Note: In a real recovery, we would delete the "loser" file here
                }
            }
        }

        Self {
            root_path,
            index: Mutex::new(index),
        }
    }

    fn sec_path(&self,
        idx : SectorIdx,
        timestamp : Timestamp,
        writerank : Writerank,
        data : &[u8],
    ) -> PathBuf {
        let mut hasher = sha2::Sha256::new();
        hasher.update(data);
        let hash_result = hasher.finalize();
        self.root_storage_dir.join(
            format!("sec_{:02}_{:02x}_{:02x}_{:02x}",hash_result, idx, writerank.0, timestamp.0)
        )
    }

    fn tmp_path(&self,
        idx : SectorIdx,
        timestamp : Timestamp,
        writerank : Writerank,
        data : &[u8]
    ) -> PathBuf {
        todo!();
    }

    async fn sync_root(&self) -> std::io::Result<()> { //this syncs direcotry, not file!
        let filer = File::open(&self.root_storage_dir).await?;
        filer.sync_data().await?;
        Ok(())
    }

    async fn put(&mut self, key : &str, value : &[u8]) -> Result<(), String> {
        todo!("Not yet checked!");
        if key.len() > 255 {
            return Err("Key provided is too long!".to_string());
        }
        //if value.len() > 65535 {
        //    return Err("Value provided is too long!".to_string());
        //}

        let key_as_bytes = key.as_bytes();
        let key_len = key.len() as u32;
        let mut my_vect = Vec::with_capacity(4 + key.len() + value.len());
        my_vect.extend_from_slice(&key_len.to_be_bytes());
        my_vect.extend_from_slice(key_as_bytes);
        my_vect.extend_from_slice(value);
        //reasons we hash:
        //1) We want to stop user from giving forbidden input - like "my/input/xd"
        //2) But using directly base64 on a file that has let's say 255 bytes make it too long 
        //this is why we need to use this approach

        let mut hasher = Sha256::new();
        hasher.update(&my_vect);
        let verifier = hasher.finalize();//this way we have this hash ready. It is 32 bytes long

        let stringify = |x : std::io::Error| x.to_string();
        
        //we now write this data to the temporary file in case our system crashes????
        { //different scope so that file is dropped fast
            let mut file = File::create(&self.tmp_file_path).await.map_err(|x| x.to_string())?;
            file.write_all(&verifier).await.map_err(stringify)?;
            file.write_all(&my_vect).await.map_err(stringify)?;
            file.sync_data().await.map_err(stringify)?;
        }
        //we sync the DIRECTORY, NOT file
        self.sync_root().await.map_err(stringify)?;

        let dest_path = self.safe_key_path(key);
        let file_existance = dest_path.exists();

        {
            let mut file = File::create(&dest_path).await.map_err(stringify)?;
            file.write_all(value).await.map_err(stringify)?;
            file.sync_data().await.map_err(stringify)?;
        }

        if !file_existance {
            self.sync_root().await.map_err(stringify)?;
        }
        
        fs::remove_file(&self.tmp_file_path).await.map_err(stringify)?;
        self.sync_root().await.map_err(stringify)?;
        Ok(())
    }

    async fn get(&self, timestamp : Timestamp, writerank : Writerank) -> Option<Vec<u8> > {
        let sec_path = format!("sec_{}_{}_{}",idx, timestamp.0, writerank.0);
        match fs::read(sec_path).await {
            Ok(data) => Some(data), 
            Err(_) => None,
        }
    }

    async fn remove(&mut self, key : &str) -> bool {
        todo!("Fix this implementation. This should not take key as input");
        let path = self.sec_path(key);
        match fs::remove_file(path).await {
            Ok(_) => {
                self.sync_root().await.unwrap();
                true
            }
            Err(_) => false
        }
    }

    /// Creates a new instance of stable storage.
    //this function is also resposible for recovery
    /// Path parameter points to a directory to which this method has exclusive access.
    async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> { 
        todo!("All of this is very wrong. Think of it");
        tood!("Use this code to do recovery logic on tmp* files");
        let safe_storage = StableMem::new(root_storage_dir);
        
        if !safe_storage.tmp_file_path.exists() {
            //if there is no temp file then there's nothing we can do
            return Box::new(safe_storage)
        }
        //We now assume that there is a temp file
        let reading = fs::read(&safe_storage.tmp_file_path).await.unwrap();
        //we now decrypt 4 + key.len() + value.len()

        if reading.len() < 36 {
            let stringify = |x : std::io::Error| x.to_string();
            fs::remove_file(&safe_storage.tmp_file_path).await.map_err(stringify).unwrap();
            safe_storage.sync_root().await.map_err(stringify).unwrap();
            return Box::new(safe_storage);
        }

        let verifier = &reading[0..32];

        let mut hasher = Sha256::new();
        hasher.update(&reading[32..]);
        let new_verifier = hasher.finalize();

        if new_verifier.as_slice() == verifier {
            //we can do a recovery
            let array : [u8; 4] = reading[32..36].try_into().unwrap();
            let key_length : u32 = u32::from_be_bytes(array);

            let key : &str = str::from_utf8(&reading[36..(36+key_length as usize)]).unwrap(); 
            let dest_path = safe_storage.safe_key_path(key);
            let value : &[u8] = &reading[36+ (key_length as usize)..];
            
            let stringify = |x : std::io::Error| x.to_string();
            {
                let mut file = File::create(&dest_path).await.map_err(stringify).unwrap();
                file.write_all(value).await.map_err(stringify).unwrap();
                file.sync_data().await.map_err(stringify).unwrap();
            }
        }

        let stringify = |x : std::io::Error| x.to_string();
        fs::remove_file(&safe_storage.tmp_file_path).await.map_err(stringify).unwrap();
        safe_storage.sync_root().await.map_err(stringify).unwrap();

        Box::new(safe_storage)
    }
}

pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    Arc::new(SecMan::new(path))
}
