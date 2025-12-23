use crate::domain::*;
use crate::SystemRegisterCommand;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio::sync::{RwLock, Mutex};

/*
* Stubborn links
* Stubborn best effort broadcast
* Spawns the tasks itself
*
*
* MAKE A TASK SPANNER with Arc<Mutex<WriteHalf>>
* Co timeout wysyła wiadomość
* Jak dostanie to przerywa i się kończy
*
* Mamy jeden wątek, który słucha acków
*
*
* Ta druga strona jak otrzyma wiadomość, to jako potwiedzenie odsyła Ack z id wiadomości
* Od strony registerClinet słuchamy po półówce połączenia
* I wysyłamy do odpowiednich spammerów dzieląc do Uuid z msg headera
*
*
* Podczas recovery trzeba miec 2 etapy
* Etap na polaczenie sie
*
* i etap na tcp connection
* Jesli jest polaczenie to wiadomosc
* Jak nie ma to probojemy sie polaczyc
*
* Jak sie klient rozlaczy to dostajemy errora - co wtedy?
* Musimy czasami wrócić do nasłuchiwania
*
* Node 1 ----------------- Node 2
* 1 chce wyslac do 2
* 1 (ja) ma taska co nasłuchuje po TCP (listener)
* Robię tez drugą rzecz - wysyłać registerCommand po TCP 
* Q: Kto jest odpowiedzialny za wykrycie, że połączenie sie przerwało?
* Ans: Apparently listener bo:
*
* Jak robię w petli deserialize to read_data 
*/


/// We do not need any public implementation of this trait. It is there for use
/// in `AtomicRegister`. In our opinion it is a safe bet to say some structure of
/// this kind must appear in your solution.
#[async_trait::async_trait]
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

pub struct TcpHandler {
    tcp_locations : Vec<String>,
    map : RwLock<HashMap<u8, Arc<Mutex<TcpStream>>>>,
}

impl TcpHandler {
    pub fn new(tcp_addresses : Vec<(String,u16)>) -> Self {
        unimplemented!();
    }
    
    fn try_establish_connection() -> Mutex<TcpStream> {
        {
            let reader = self.map().read().await;
            if let Some(connection) = reader.get(&)
        }
    }
}

#[async_trait::async_trait]
impl RegisterClient for TcpHandler {
    async fn send(&self, msg: Send) {
        let connection = self.try_establish_connection(msg.target);
        todo!("connection.send(msg) or something like that");
    }

    async fn broadcast(&self, msg: Broadcast) {
        let connections : Vec<????????> = self.tcp_locations.iter()
            .map(|name| todo!("Map this name to a valid u8 process id"))
            .map(|u8_name| self.try_establish_connection(u8_name))
            .collect();
    }
}


/* todo! 
*
* Think whether codomain type of map is good
*
* I just want this module to handle messaging sending, nothing more to be honest.
* This should be straighforward given that I already have deserialization etc.
*
* This should be a nice, abstraction that does not show its flesh
*
* I should implement the spammer
*/
