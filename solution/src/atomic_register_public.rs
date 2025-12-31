use crate::domain::*;
use crate::register_client_public::{RegisterClient, Broadcast};
use crate::register_client_public::Send as SendRCP;
use crate::sectors_manager_public::SectorsManager;
use uuid::Uuid;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio::task::JoinHandle;

const RETRY_INTERVAL : Duration = Duration::from_millis(1000);

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

type ClientCallback = Box<dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Copy)]
enum OperationType {
    Read,
    Write,
}

struct AtomicReg {
    setup : Setup,
    sector_data : RamSecData,
    broadcaster : StubbornBroadcaster,

    request_identifier: u64,
    msg_ident : Uuid,
    is_executing: bool,
    op_type: Option<OperationType>,
    client_val: Option<SectorVec>, //When write is SecData
    callback: Option<ClientCallback>,
    queue : VecDeque<(ClientRegisterCommand, ClientCallback)>,//A queue for messages that we get
    
    //this map is for checking ACKs
    read_list: HashMap<u8, (u64, u8, SectorVec)>,
    ack_list: HashSet<u8>,
    write_phase: bool, // false = Read Phase, true = Write Phase
    
    reading_val: Option<SectorVec>, //The value we return to the client (for Read ops)
    target_ts: u64,                 //New timestamp
    target_wr: u8,                  //Writer rank for new timestamp
    target_val: Option<SectorVec>,  //Value we want to impose
}

struct Setup {
    self_rank: u8,
    sector_idx: u64,
    processes_count: u8,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<dyn RegisterClient>,
}

struct RamSecData {
    timestamp : u64,
    writerank : u8,
    data : SectorVec, 
}

pub struct StubbornBroadcaster {
    task: Option<JoinHandle<()>>,
}

impl AtomicReg {
    async fn new(
        self_rank: u8,
        sector_idx: u64,
        sectors_manager: Arc<dyn SectorsManager>,
        register_client: Arc<dyn RegisterClient>,
        processes_count: u8,
    ) -> Self {
        let (ts, wr) = sectors_manager.read_metadata(sector_idx).await;
        let val = sectors_manager.read_data(sector_idx).await;
        
        Self {
            setup : Setup {
                self_rank,
                sector_idx,
                processes_count,
                sectors_manager,
                register_client,
            },
            sector_data : RamSecData {
                timestamp : ts,
                writerank : wr,
                data : val,
            },
            broadcaster : StubbornBroadcaster::new(),
            request_identifier: 0,
            msg_ident : uuid::Uuid::nil(),
            is_executing: false,
            op_type: None,
            client_val: None,
            callback: None,
            queue : VecDeque::new(),
            read_list: HashMap::new(),
            ack_list: HashSet::new(),
            write_phase: false,
            reading_val: None,
            target_ts: 0,
            target_wr: 0,
            target_val: None,
        }
    }
    
    async fn handle_read_proc(&self, target : u8, msg_ident : Uuid) {
        let reply = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier : self.setup.self_rank,
                msg_ident,
                sector_idx : self.setup.sector_idx,
            },
            content: SystemRegisterCommandContent::Value {
                timestamp: self.sector_data.timestamp,
                write_rank: self.sector_data.writerank,
                sector_data: self.sector_data.data.clone(),
            },
        };
        
        self.setup.register_client.send(SendRCP {
            cmd: Arc::new(reply),
            target,
        }).await;
    }

    async fn handle_value(&mut self, ts : u64, wr : u8, sec_data : SectorVec, msg_ident : Uuid, proc_id : u8) {
        if !self.is_executing || self.write_phase || msg_ident != self.msg_ident {
            return;
        }

        self.read_list.insert(proc_id, (ts, wr, sec_data));

        if self.read_list.len() > (self.setup.processes_count as usize / 2) {
            self.broadcaster.abort();

            let mut max_ts = 0;
            let mut max_wr = 0;
            let mut max_val = None;
            for (_, (ts, wr, val)) in &self.read_list {
                if *ts > max_ts || (*ts == max_ts && *wr > max_wr) {
                    max_ts = *ts;
                    max_wr = *wr;
                    max_val = Some(val.clone());
                }
            }
            
            //What if max_val is None
            let best_data = max_val.unwrap_or_else(|| self.sector_data.data.clone());

            let send_data;
            if let Some(OperationType::Read) = self.op_type {
                //READ: We perform Read-Repair. Write back the highest value found
                self.target_ts = max_ts;
                self.target_wr = max_wr;
                self.target_val = Some(best_data.clone());
                self.reading_val = Some(best_data.clone());
                send_data = best_data;
            } else {
                //WRITE: We pick a new timestamp higher than what we saw
                self.target_ts = max_ts + 1;
                self.target_wr = self.setup.self_rank;
                self.target_val = Some(self.sector_data.data.clone());
                send_data = self.client_val.clone().unwrap();
            }

            self.write_phase = true;
            self.ack_list.clear();
            self.msg_ident = uuid::Uuid::new_v4();

            let msg = SystemRegisterCommand {
                header: SystemCommandHeader {
                    process_identifier: self.setup.self_rank,
                    msg_ident: self.msg_ident,
                    sector_idx: self.setup.sector_idx,
                },
                content: SystemRegisterCommandContent::WriteProc {
                    timestamp: self.target_ts,
                    write_rank: self.target_wr,
                    data_to_write: send_data,
                },
            };
            self.broadcaster.start(self.setup.register_client.clone(), msg);
        }
    }

    async fn handle_write_proc(&mut self, ts : u64, wr : u8, new_data : SectorVec, proc_id : u8, msg_ident : Uuid) {
        if ts > self.sector_data.timestamp || (ts == self.sector_data.timestamp && wr > self.sector_data.writerank) {
            self.sector_data.timestamp = ts;
            self.sector_data.writerank = wr;
            self.sector_data.data = new_data;
            self.setup.sectors_manager.write(self.setup.sector_idx, &(self.sector_data.data.clone(), ts,  wr)).await;
        }

        let reply = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier : self.setup.self_rank,
                msg_ident,
                sector_idx : self.setup.sector_idx,
            },
            content: SystemRegisterCommandContent::Ack,
        };

         self.setup.register_client.send(SendRCP {
            cmd : Arc::new(reply),
            target : proc_id,
        }).await;
    }

    async fn handle_ack(&mut self, proc_id : u8, msg_ident : Uuid) {
        if !self.is_executing || !self.write_phase || msg_ident != self.msg_ident {
            return;
        }
        self.ack_list.insert(proc_id);

        if self.ack_list.len() > (self.setup.processes_count as usize / 2) {
            self.broadcaster.abort();
            if let Some(cb) = self.callback.take() {
                let response = ClientCommandResponse {
                    request_identifier: self.request_identifier,
                    status: StatusCode::Ok,
                    op_return: match self.op_type {
                        Some(OperationType::Read) => OperationReturn::Read {
                            read_data: self.reading_val.clone().unwrap() 
                        },
                        Some(OperationType::Write) => OperationReturn::Write,
                        None => unreachable!(),
                    },
                };
                cb(response).await;
            }
            self.is_executing = false;
            //We now handle another command
            if let Some((next_cmd, next_callback)) = self.queue.pop_front() {
                self.client_command(next_cmd, next_callback).await;
            }
        }
    }
}

#[async_trait::async_trait]
impl AtomicRegister for AtomicReg {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: ClientCallback
    ) {
        //If there is some task in progress just push the value on the queue
        if self.is_executing {
            self.queue.push_back((cmd, success_callback));
            return;
        }
        self.is_executing = true;
        self.request_identifier = cmd.header.request_identifier;
        self.callback = Some(success_callback);
        self.read_list.clear();
        self.ack_list.clear();
        self.write_phase = false; // Always start with Read Phase
        self.msg_ident = uuid::Uuid::new_v4();

        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.op_type = Some(OperationType::Read);
                self.client_val = None;
            },
            ClientRegisterCommandContent::Write { data } => {
                self.op_type = Some(OperationType::Write);
                self.client_val = Some(data);
            }
        }
        let msg = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.setup.self_rank,
                msg_ident: self.msg_ident,
                sector_idx: self.setup.sector_idx,
            },
            content: SystemRegisterCommandContent::ReadProc,
        };

        self.broadcaster.start(self.setup.register_client.clone(), msg);
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => {
                self.handle_read_proc(cmd.header.process_identifier, cmd.header.msg_ident).await;
            },
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } => {
                self.handle_value(timestamp, write_rank, sector_data, cmd.header.msg_ident, cmd.header.process_identifier).await;
            },
            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                self.handle_write_proc(timestamp, write_rank, data_to_write, cmd.header.process_identifier, cmd.header.msg_ident).await;
            },
            SystemRegisterCommandContent::Ack => {
                self.handle_ack(cmd.header.process_identifier, cmd.header.msg_ident).await;
            }
        }
    }
}

impl StubbornBroadcaster {
    pub fn new() -> Self {
        Self { task: None }
    }

    pub fn start(&mut self, client: Arc<dyn RegisterClient>, cmd: SystemRegisterCommand) {
        self.abort();

        let cmd_arc = Arc::new(cmd);

        self.task = Some(tokio::spawn(async move {
            loop {
                client.broadcast(Broadcast { cmd: cmd_arc.clone() }).await;
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        }));
    }

    pub fn abort(&mut self) {
        if let Some(handle) = self.task.take() {
            handle.abort();
        }
    }
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
    Box::new(AtomicReg::new(
        self_ident,
        sector_idx,
        sectors_manager,
        register_client,
        processes_count,
    ).await)
}


