### A second big assignment of the distributed systems class of 25/26

## Overview

This project implements a distributed **(N,N)-Atomic Register** system. It allows multiple concurrent clients to read and write 4096-byte data sectors across a cluster of nodes. The system guarantees linearizability (atomicity) even in the presence of node crashes and network partitions, provided a majority of nodes remain accessible.

The core logic follows the **Read-Impose Write-Majority** algorithm, ensuring that once a write is acknowledged, any subsequent read operation will return that value or a newer one.

## Architecture

The system is modular, using `tokio` for asynchronous execution. Communication is split into internal (system-to-system) and external (client-to-system) paths, both secured via HMAC.

### Key Components

* **`lib.rs`**: The main entry point. It bootstraps the TCP listener, initializes the storage manager, and spawns the network handling tasks.
* **`atomic_register_public.rs`**: The core state machine. It handles the Read/Write phases of the algorithm. It uses a "Stubborn Broadcaster" to ensure messages eventually reach a quorum of nodes.
* **`sector_registry.rs`**: A thread-safe map that manages active `AtomicRegister` instances in memory. Registers are **lazy-loaded**: a register is only created when a command for its specific `SectorIdx` is received.
* **`register_client_public.rs`**: Handles the lower-level details of sending messages to other nodes. It manages a pool of TCP connections and handles re-connection logic if a peer drops.
* **`sectors_manager_public.rs`**: The persistence layer. It ensures durability by writing to temporary files and using `fs::rename` (atomic on POSIX) to swap them into place. It also handles **Crash Recovery** by scanning the directory on startup and cleaning up incomplete writes.
* **`network_handler.rs`**: The TCP server that deserializes incoming commands, verifies HMAC signatures, and routes commands to the appropriate register.


<img src="./dependency_graph.svg" alt="Dependency Graph" width="600">
