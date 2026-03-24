# GoDFS - Go Distributed File System

A distributed file system inspired by Hadoop HDFS, built with Go and gRPC. GoDFS provides a scalable, fault-tolerant storage solution that distributes files across multiple nodes with automatic replication for high availability.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [System Components](#system-components)
- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Reliability Demo](#reliability-demo)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Docker Support](#docker-support)
- [Project Structure](#project-structure)
- [Development](#development)

## Overview

GoDFS is a distributed file system designed to handle large files by splitting them into smaller blocks and distributing them across multiple DataNodes. The system automatically replicates data across nodes to ensure fault tolerance and high availability. Communication between all components is handled via gRPC for high performance and efficiency.

### Key Characteristics

- **Block-based Storage**: Files are split into configurable-sized blocks (default 3KB for testing, configurable to 32MB or more)
- **Replication Factor**: Each block is replicated 3 times across different DataNodes by default
- **Master-Slave Architecture**: Single NameNode coordinates multiple DataNodes
- **Concurrent Operations**: Leverages Go's concurrency for parallel block processing
- **gRPC Communication**: Fast, efficient inter-node communication using Protocol Buffers

## Architecture

```
                                 ┌──────────────┐
                                 │   NameNode   │
                                 │   (Master)   │
                                 └──────┬───────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
            ┌───────▼───────┐   ┌───────▼───────┐   ┌──────▼────────┐
            │   DataNode 1  │   │   DataNode 2  │   │  DataNode N   │
            │    (Slave)    │   │    (Slave)    │   │   (Slave)     │
            └───────────────┘   └───────────────┘   └───────────────┘
                    ▲                   ▲                   ▲
                    │                   │                   │
                    └───────────────────┼───────────────────┘
                                        │
                                 ┌──────┴───────┐
                                 │    Client    │
                                 └──────────────┘
```

## Key Features

### 1. Distributed Storage
- Files are automatically split into blocks and distributed across multiple DataNodes
- Load balancing ensures even distribution of blocks across available nodes
- Each DataNode stores its blocks in isolated directories

### 2. Fault Tolerance
- **Replication**: Each block is replicated 3 times (configurable) across different DataNodes
- **Block Reports**: DataNodes send periodic heartbeats (every 10 seconds) to the NameNode
- **Self-Healing Replication**: NameNode marks stale DataNodes as dead and re-replicates under-replicated blocks
- **Checksum Validation**: Clients verify block checksums and fall back to other replicas on mismatch
- **Metadata Redundancy**: NameNode maintains comprehensive metadata about all blocks and their locations

### 3. High Performance
- **Parallel Processing**: Concurrent block upload/download using Go goroutines
- **gRPC**: Low-latency communication protocol
- **Direct Data Transfer**: Clients communicate directly with DataNodes for data operations

### 4. Scalability
- Easy to add new DataNodes to the cluster
- NameNode tracks all nodes dynamically
- Load balancing based on block count per DataNode

## System Components

### 1. NameNode (Master)

The NameNode is the central coordinator of the GoDFS cluster.

**Responsibilities:**
- **Metadata Management**: Maintains file-to-block mappings and block-to-DataNode mappings
- **DataNode Registry**: Tracks all active DataNodes and their health status
- **Block Allocation**: Selects optimal DataNodes for new block storage based on load
- **Read Coordination**: Provides clients with DataNode locations for file reads

**Key Data Structures:**
```go
type NameNodeData struct {
    BlockSize                 int64                        // Size of each block
    DataNodeToBlockMapping    map[string][]string          // DataNode ID -> Block IDs
    ReplicationFactor         int64                        // Number of replicas (default: 3)
    DataNodeToMetadataMapping map[string]DataNodeMetadata  // DataNode metadata
    FileToBlockMapping        map[string][]string          // File path -> Block IDs
}
```

**gRPC Services:**
- `Register_DataNode`: Register new DataNodes with the cluster
- `GetAvailableDatanodes`: Return least-loaded DataNodes for block placement
- `BlockReport`: Receive periodic block inventory from DataNodes
- `GetDataNodesForFile`: Return DataNode locations for file blocks
- `FileBlockMapping`: Store file-to-block metadata

### 2. DataNode (Slave)

DataNodes are the worker nodes that store actual file data.

**Responsibilities:**
- **Data Storage**: Store blocks as files in the local filesystem
- **Block Management**: Track all blocks stored locally
- **Heartbeat**: Send periodic block reports to NameNode (every 10 seconds)
- **Data Operations**: Handle read/write requests from clients
- **Registration**: Auto-register with NameNode on startup

**Key Operations:**
```go
type DataNode struct {
    ID               string    // Unique UUID for this DataNode
    DataNodeLocation string    // Local storage directory
    Blocks           []string  // List of block IDs stored locally
}
```

**gRPC Services:**
- `SendDataToDataNodes`: Receive and store blocks from clients
- `ReadBytesFromDataNode`: Serve block data to clients

**Storage Structure:**
```
datanode-files/
└── <datanode-uuid>/
    ├── <block-id-1>.txt
    ├── <block-id-2>.txt
    └── <block-id-n>.txt
```

### 3. Client

The client provides the interface for file operations.

**Responsibilities:**
- **File Upload (Write)**: Split files into blocks and distribute to DataNodes
- **File Download (Read)**: Retrieve blocks from DataNodes and reconstruct files
- **Metadata Sync**: Communicate file-to-block mappings to NameNode

**Key Features:**
- Concurrent block uploads using goroutines
- Maintains block ordering for correct file reconstruction
- Random DataNode selection for reads (load distribution)
- Automatic retry logic

## How It Works

### Write Operation Flow

1. **Client Initiation**
   - Client connects to NameNode
   - File is split into blocks of configured size (e.g., 3KB)
   - Each block gets a unique UUID

2. **DataNode Selection**
   - For each block, client requests NameNode for available DataNodes
   - NameNode returns 3 least-loaded DataNodes (replication factor)

3. **Concurrent Upload**
   - Client uploads block to all 3 DataNodes concurrently
   - Each DataNode stores block as `<block-id>.txt`
   - DataNodes acknowledge successful storage

4. **Metadata Registration**
   - Client sends file-to-block mapping to NameNode
   - NameNode stores: `filename -> [block1, block2, ..., blockN]`

5. **Block Reporting**
   - DataNodes periodically report their blocks to NameNode
   - NameNode updates its block-to-DataNode mappings

```
Client → NameNode: "Give me 3 DataNodes"
NameNode → Client: [DN1, DN2, DN3]
Client → DN1, DN2, DN3: Send Block (parallel)
Client → NameNode: "File X has blocks [B1, B2, B3]"
```

### Read Operation Flow

1. **Client Request**
   - Client requests file from NameNode
   - NameNode looks up file-to-block mapping

2. **DataNode Location**
   - NameNode returns block IDs and available DataNodes for each block
   - Example: `Block1 → [DN1, DN2, DN3]`

3. **Data Retrieval**
   - Client randomly selects one DataNode per block
   - Retrieves blocks sequentially in correct order
   - Displays/stores reconstructed file content

```
Client → NameNode: "Where is File X?"
NameNode → Client: Block1@[DN1,DN2,DN3], Block2@[DN2,DN4,DN5]
Client → DN1: "Send Block1"
Client → DN2: "Send Block2"
Client: Reconstruct File from Blocks
```

## Prerequisites

- **Go**: Version 1.21.7 or higher
- **Protocol Buffers Compiler** (`protoc`): For generating gRPC code
- **Make**: For using the Makefile commands
- **Docker** (optional): For containerized deployment

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Raghav-Tiruvallur/GoDFS.git
cd GoDFS
```

### 2. Install Dependencies

```bash
go mod download
```

### 3. Generate Protocol Buffer Files

```bash
make protoc
```

This runs:
```bash
protoc namenode.proto --go_out=paths=source_relative:./proto/namenode --go-grpc_out=paths=source_relative:./proto/namenode --proto_path=./proto/namenode
protoc datanode.proto --go_out=paths=source_relative:./proto/datanode --go-grpc_out=paths=source_relative:./proto/datanode --proto_path=./proto/datanode
```

### 4. Build the Binary

```bash
make build
```

This creates the `go-dfs` executable.

## Usage

### Starting the Cluster

#### 1. Start the NameNode

```bash
make run-namenode
```

Or manually:
```bash
./go-dfs namenode -port 8080 -block-size 32
```

**Parameters:**
- `-port`: Port for NameNode to listen on (default: 8080)
- `-block-size`: Block size in MB (default: 32)

#### 2. Start DataNodes

```bash
make run-datanodes
```

This launches 10 DataNodes on ports 8001-8010.

Or manually start individual DataNodes:
```bash
./go-dfs datanode -port 8001 -location ./datanode-files
./go-dfs datanode -port 8002 -location ./datanode-files
./go-dfs datanode -port 8003 -location ./datanode-files
```

**Parameters:**
- `-port`: Port for DataNode to listen on
- `-location`: Directory path for storing blocks

### Client Operations

#### Write a File

```bash
make run-client-write
```

Or manually:
```bash
./go-dfs client -namenode 8080 -operation write -source-path . -filename big.txt
```

**Parameters:**
- `-namenode`: NameNode port (default: 8080)
- `-operation`: Operation type (`write` or `read`)
- `-source-path`: Directory containing the file
- `-filename`: Name of the file to write

#### Read a File

```bash
make run-client-read
```

Or manually:
```bash
./go-dfs client -namenode 8080 -operation read -source-path . -filename big.txt
```

**Parameters:**
- `-namenode`: NameNode port (default: 8080)
- `-operation`: Operation type (`read`)
- `-source-path`: Directory path (for metadata)
- `-filename`: Name of the file to read

## Configuration

### Block Size

The block size determines how files are chunked. Configure it when starting the NameNode:

```bash
./go-dfs namenode -port 8080 -block-size 64  # 64 MB blocks
```

**Note**: In the code, the client uses a hardcoded block size of 3KB for testing purposes. For production, this should match the NameNode configuration.

### Replication Factor

Currently hardcoded to 3 in the NameNode. To change it, modify:

```go
// namenode/namenode.go
nameNode.ReplicationFactor = 3  // Change this value
```

### Heartbeat Interval

DataNodes send block reports every 10 seconds. To modify:

```go
// datanode/datanode.go
interval := 10 * time.Second  // Adjust this value
```

## Reliability Demo

### 1) Checksum fallback demo

This proves that corrupted replicas are rejected and healthy replicas are used automatically.

```bash
# baseline
./go-dfs client -namenode 8080 -operation write -source-path . -filename test.txt
./go-dfs client -namenode 8080 -operation read -source-path . -filename test.txt > read_ok.txt
diff -u test.txt read_ok.txt

# pick one block ID and list replicas
BLOCK="your-block-id.txt"
find custom-dn-storage -type f -name "$BLOCK"

# corrupt two replicas (leave one healthy)
echo "CORRUPTED" >> "custom-dn-storage/<uuid1>/$BLOCK"
echo "CORRUPTED" >> "custom-dn-storage/<uuid2>/$BLOCK"

# read still succeeds via fallback
./go-dfs client -namenode 8080 -operation read -source-path . -filename test.txt > read_after_corrupt.txt
diff -u test.txt read_after_corrupt.txt
```

Expected result:
- Diff output is empty (file content still matches).
- Client may log checksum mismatch for bad replicas.

### 2) Automatic re-replication demo

This proves dead DataNodes are detected and blocks are re-replicated automatically.

```bash
# start namenode + 4 datanodes
./go-dfs namenode -port 8080 -block-size 32
./go-dfs datanode -port 8001 -location ./custom-dn-storage
./go-dfs datanode -port 8002 -location ./custom-dn-storage
./go-dfs datanode -port 8003 -location ./custom-dn-storage
./go-dfs datanode -port 8004 -location ./custom-dn-storage

# write data
./go-dfs client -namenode 8080 -operation write -source-path . -filename test.txt
```

Then stop one DataNode process (Ctrl+C), wait ~30 seconds, and check NameNode logs.

Expected logs:
- `Marked datanode ... as Dead (heartbeat timeout)`
- `Re-replicated block ... from ... to ...`

Final validation:

```bash
./go-dfs client -namenode 8080 -operation read -source-path . -filename test.txt > read_after_rerep.txt
diff -u test.txt read_after_rerep.txt
```

Expected result:
- Diff output is empty after the node failure and re-replication.

## API Reference

### NameNode gRPC Services

```protobuf
service NamenodeService {
    rpc getAvailableDatanodes (google.protobuf.Empty) returns (freeDataNodes);
    rpc register_DataNode(datanodeData) returns (status);
    rpc getDataNodesForFile (fileData) returns (blockData);
    rpc BlockReport (datanodeBlockData) returns (status);
    rpc FileBlockMapping (fileBlockMetadata) returns (status);
}
```

### DataNode gRPC Services

```protobuf
service DatanodeService {
    rpc sendDataToDataNodes(clientToDataNodeRequest) returns (Status);
    rpc ReadBytesFromDataNode(blockRequest) returns (byteResponse);
}
```

## Docker Support

### Building the Base Image

```bash
docker build -t godfs-base -f Dockerfile .
```

### Running NameNode Container

```bash
docker build -t godfs-namenode -f namenode/Dockerfile .
docker run -p 8080:8080 godfs-namenode
```

### Running DataNodes Container

```bash
docker build -t godfs-datanode -f datanode/Dockerfile .
docker run godfs-datanode
```

**Note**: The DataNode Dockerfile uses the `run_datanodes.sh` script which may need adjustment for containerized environments.

## Project Structure

```
GoDFS-main/
├── main.go                      # Entry point with CLI argument parsing
├── go.mod                       # Go module dependencies
├── go.sum                       # Dependency checksums
├── Makefile                     # Build and run commands
├── Dockerfile                   # Base Docker image
├── README.md                    # Basic documentation
├── big.txt                      # Test file (large)
├── test.txt                     # Test file (small)
│
├── client/
│   └── client.go                # Client implementation (read/write operations)
│
├── namenode/
│   ├── namenode.go              # NameNode implementation (metadata management)
│   └── Dockerfile               # NameNode container image
│
├── datanode/
│   ├── datanode.go              # DataNode implementation (storage management)
│   └── Dockerfile               # DataNode container image
│
├── proto/
│   ├── namenode/
│   │   └── namenode.proto       # NameNode gRPC service definitions
│   └── datanode/
│       └── datanode.proto       # DataNode gRPC service definitions
│
├── scripts/
│   ├── generate_proto.sh        # Generate Go code from proto files
│   └── run_datanodes.sh         # Launch multiple DataNodes
│
└── utils/
    └── utils.go                 # Utility functions (error handling, helpers)
```

## Development

### Adding a New Feature

1. **Modify Protocol Buffers** (if needed)
   - Edit `.proto` files in `proto/namenode/` or `proto/datanode/`
   - Run `make protoc` to regenerate Go code

2. **Implement Service Methods**
   - Add methods to NameNode or DataNode structs
   - Implement corresponding gRPC handlers

3. **Update Client**
   - Add client-side logic in `client/client.go`

4. **Test**
   - Build: `make build`
   - Test with sample files

### Code Style

- Follow Go conventions and idioms
- Use meaningful variable names
- Add comments for complex logic
- Handle errors appropriately (currently uses panic, consider graceful error handling for production)

### Known Limitations

1. **Single NameNode**: No NameNode redundancy (single point of failure)
2. **Limited Error Handling**: Uses panic in several paths instead of graceful error recovery
3. **Hardcoded Values**: Block size and replication factor are not fully configurable
4. **No Authentication**: No security or access control mechanisms
5. **Memory Constraints**: Entire blocks are loaded into memory during transfers

### Future Enhancements

- [ ] Implement secondary NameNode for high availability
- [ ] Add authentication and authorization
- [ ] Support for larger files with streaming
- [ ] Web UI for cluster monitoring
- [ ] Configurable replication factor per file
- [ ] Load balancing for read operations
- [ ] Compression support
- [ ] Erasure coding for storage efficiency

## Troubleshooting

### NameNode Won't Start
- Ensure port 8080 (or specified port) is not in use
- Check logs for binding errors

### DataNodes Not Registering
- Verify NameNode is running before starting DataNodes
- Check network connectivity to NameNode port
- Review NameNode logs for registration attempts

### File Upload Fails
- Ensure sufficient DataNodes are running (at least 3 for default replication)
- Check DataNode storage permissions
- Verify file exists at specified source path

### File Read Returns Empty/Corrupted Data
- Verify blocks exist on DataNodes
- Check block reports are being sent (every 10 seconds)
- Ensure file-to-block mapping was registered correctly

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request
