use sha2::{Sha256};
use crate::domain::*;
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
    let (payload, hmac_tag) = rest_of_package.split_at(split_numb as usize);

    let (deserialized, _size_of_cmd_bytes) = bincode::serde::decode_from_slice(
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

    type HmacSha256 = Hmac<Sha256>;
    let mut verifier = HmacSha256::new_from_slice(relevant_hmac_key)
        .map_err(|e| DecodingError::IoError(Error::other(e)))?;
    
    verifier.update(&payload);

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
