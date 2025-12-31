use crate::domain::*;
use crate::transfer_public::serialize_register_command;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc;

const WORKER_QUEUE_SIZE : usize = 1024;
const RETRY_DELAY : Duration = Duration::from_millis(500);
//todo! Make sure this size makes sense!
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

pub(crate) struct TcpHandler {
    tcp_locations : Vec<(String,u16)>,
    self_proc : u8,
    self_channel : Sender<SystemRegisterCommand>,
    workers_map : RwLock<HashMap<u8, Sender<Arc<SystemRegisterCommand>>>>,
    hmac_system : [u8; 64],
}

impl TcpHandler {
    pub fn new(
        tcp_locations : Vec<(String,u16)>,
        self_proc : u8,
        self_channel : Sender<SystemRegisterCommand>,
        hmac_system : [u8; 64],
    ) -> Self {
        Self {
            tcp_locations,
            self_proc,
            self_channel,
            workers_map : RwLock::new(HashMap::new()),
            hmac_system,    
        }
    }
    
    ///It is assumed that this function is only inovoked when target_proc =/= self.self_proc 
    async fn worker_getter(&self, target_proc : u8) -> Sender<Arc<SystemRegisterCommand>> {
        if target_proc == self.self_proc {
            unreachable!();
        }
        if let Some(tx) = self.workers_map
            .read()
            .await
            .get(&target_proc) {

            return tx.clone();
        }//Does this look good? I think it's about personal taste

        let (tx, rx) = mpsc::channel(WORKER_QUEUE_SIZE);
        let address = self.get_address(target_proc);
        let hmac = self.hmac_system;
        tokio::spawn(worker_task(target_proc, address, rx, hmac));
        let mut guard = self.workers_map.write().await;
        guard.insert(target_proc, tx.clone());
        tx
    }

    fn get_address(&self, target_proc : u8) -> String {
        let (host, port) = &self.tcp_locations[(target_proc - 1) as usize];
        format!("{}:{}", host, port)
    }
}

#[async_trait::async_trait]
impl RegisterClient for TcpHandler {
    async fn send(&self, msg: Send) {
        if msg.target == self.self_proc {
            self.self_channel.send((*msg.cmd).clone()).await;
        } else {
            let tx = self.worker_getter(msg.target).await;
            if let Err(_) = tx.send(msg.cmd).await {
                log::error!("We cannot send a message to {}",msg.target);
            }
        }
    }

    async fn broadcast(&self, msg: Broadcast) {
        for target in 1..=self.tcp_locations.len() {
            self.send( Send {
                cmd : msg.cmd.clone(),
                target : target as u8
            }).await;
        }
    }
}

async fn worker_task(
    target_proc : u8,
    address : String,
    mut rx : mpsc::Receiver<Arc<SystemRegisterCommand>>,
    hmac : [u8; 64],
) {
    log::debug!("Worker started for process {} ({})", target_proc, address);
    let mut stream : Option<TcpStream> = None;
    while let Some(cmd) = rx.recv().await {
        let active_stream = stubborn_send_op(target_proc, &address, &hmac, cmd, stream).await;
        stream = Some(active_stream);
    }
}

//Notice that this is stubborn in a sense that despite lack of connection
//it tries to connect.
//It is NOT stubborn (yet, there is a wrapper for that) in the sense that
//it broadcasts the message until it gets ACK messages back, okay?
async fn stubborn_send_op(
    target_proc : u8,
    addr : &str,
    hmac : &[u8;64],
    cmd : Arc<SystemRegisterCommand>,
    mut stream : Option<TcpStream>,
) -> TcpStream {
    loop {
        if stream.is_none() {
            stream = try_connect(target_proc, addr).await;
        }
        if let Some(mut connection) = stream.take() {
            match try_write(&mut connection, &cmd, hmac).await {
                Ok(_) => {
                    return connection;
                },
                Err(_) => {
                    stream = None;
                    tokio::time::sleep(RETRY_DELAY).await;
                },
            }
        } else {
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

async fn try_write(
    socket : &mut TcpStream,
    cmd : &Arc<SystemRegisterCommand>,
    hmac : &[u8; 64]
) -> Result<(),()> {
    let register_command = RegisterCommand::System((**cmd).clone());
    match serialize_register_command(&register_command, socket, hmac).await {
        Ok(_) => Ok(()),
        Err(e) => {
            log::debug!("Serialization failed. socket is {:?}", e);
            Err(())
        }
    }
}

async fn try_connect(target_proc : u8, addr : &str) -> Option<TcpStream> {
    match TcpStream::connect(addr).await {
        Ok(stream) => {
            if let Err(_) = stream.set_nodelay(true) {
                log::debug!("I have no idea why this happens?");
            }
            Some(stream)
        },
        Err(e) => {
            log::debug!("No connection with proc {}, addr = {} {}",target_proc, addr,e);
            None
        }
    }
}

