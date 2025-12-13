mod domain;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;

//dependencies added by me (Bartek) below:
use bincode::serde::{encode_into_std_write};

//end of dependencies added by me

pub async fn run_register_process(config: Configuration) {
    unimplemented!()
}

pub mod atomic_register_public {
    use crate::{
        ClientCommandResponse, ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of `SystemRegisterCommand` messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Communication with other processes of the system is to be done by `register_client`.
    /// And sectors must be stored in the `sectors_manager` instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        unimplemented!()
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;

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

    Struct MySecMangaer

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        unimplemented!()
    }
}

pub mod transfer_public {
    use sha2::{Digest, Sha256};
    use crate::{ClientCommandHeader, ClientRegisterCommand, RegisterCommand, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};
    use bincode::error::{DecodeError, EncodeError};
    use std::io::Error;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use hmac::{Hmac,Mac};
    #[derive(Debug)]
    pub enum EncodingError {
        IoError(Error),
        BincodeError(EncodeError),
    }

    #[derive(Debug, derive_more::Display)]
    pub enum DecodingError {
        IoError(Error),
        BincodeError(DecodeError),
        InvalidMessageSize,
    }

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), DecodingError> {
        let mut msg_bytes = [0u8 ; 8];
        data.read_exact(&mut msg_bytes).await.map_err(|e| DecodingError::IoError(e))?;

        let msg_size = u64::from_be_bytes(msg_bytes);
        if msg_size < 32 {
            return Err(DecodingError::InvalidMessageSize);
        }

        let mut rest_of_package = vec![0u8; msg_size as usize]; 
        data.read_exact(&mut rest_of_package).await.map_err(|e| DecodingError::IoError(e))?;

        let split_numb = msg_size - 32;
        todo!("think what when size of the message is equal to 32");
        let (payload, hmac_tag) = rest_of_package.split_at(split_numb as usize);

        let (deserialized, size_of_cmd_bytes) = bincode::serde::decode_from_slice(
            &payload,
            bincode::config::standard()
                .with_big_endian()
                .with_fixed_int_encoding()
        ).map_err(|e| DecodingError::BincodeError(e))?;
        
        let relevant_hmac_key = match deserialized {
            RegisterCommand::Client(_) => {
                hmac_client_key.as_slice()
            },
            RegisterCommand::System(_) => {
                hmac_system_key.as_slice()
            }
        };

        todo!("think what about when I have different length comign from different hmac keys!");
        type HmacSha256 = Hmac<Sha256>;
        let mut verifier = HmacSha256::new_from_slice(relevant_hmac_key)
            .map_err(|e| DecodingError::IoError(Error::other(e)))?; // MAYBE LATER CHANGE ERROR
        // TYPE TODO todo! to_do TO_DO
        
        verifier.update(&payload);
        //let payload_tag = verifier.finalize().into_bytes(); //I'll leave it just in case
        
        Ok((
            deserialized,
            verifier //this is true/false
                .verify_slice(hmac_tag)
                .is_ok()
        ))
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> {
        
        match cmd {
            RegisterCommand::Client(_) => {
                if hmac_key.len() != 32 {
                    return Err(EncodingError::IoError(Error::other("bad format of client hmac key. Expected 32B")));
                }
            },
            RegisterCommand::System(_) => {
                if hmac_key.len() != 64 {
                    return Err(EncodingError::IoError(Error::other("bad format of system hmac key. Expected 64B")));
                }
            },
        };

        let payload_as_bin : Vec<u8> = bincode::serde::encode_to_vec(
            cmd,
            bincode::config::standard()
                .with_big_endian()
                .with_fixed_int_encoding()
        ).map_err(|e| EncodingError::BincodeError(e))?;

        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(hmac_key)
            .map_err(|e| EncodingError::IoError(Error::other(e)))?;
        mac.update(&payload_as_bin);
        let hmac_tag = mac.finalize().into_bytes();

        let msg_size = payload_as_bin.len() as u64 + 32; //here 32 represnts the length of hmac

        //writing starts now
        writer.write_all(&msg_size.to_be_bytes()).await.map_err(|e| EncodingError::IoError(e))?;
        writer.write_all(payload_as_bin.as_slice()).await.map_err(|e| EncodingError::IoError(e))?;
        writer.write_all(hmac_tag.as_slice()).await.map_err(|e| EncodingError::IoError(e))?;
        writer.flush().await.map_err(|e| EncodingError::IoError(e))?;
        Ok(())
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in `AtomicRegister`. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }
}
